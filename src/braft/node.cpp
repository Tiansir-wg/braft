// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include <bthread/unstable.h>
#include <brpc/errno.pb.h>
#include <brpc/controller.h>
#include <brpc/channel.h>

#include "braft/errno.pb.h"
#include "braft/util.h"
#include "braft/raft.h"
#include "braft/node.h"
#include "braft/log.h"
#include "braft/raft_meta.h"
#include "braft/snapshot.h"
#include "braft/file_service.h"
#include "braft/builtin_service_impl.h"
#include "braft/node_manager.h"
#include "braft/snapshot_executor.h"
#include "braft/errno.pb.h"

namespace braft
{

    DEFINE_int32(raft_max_election_delay_ms, 1000,
                 "Max election delay time allowed by user");
    BRPC_VALIDATE_GFLAG(raft_max_election_delay_ms, brpc::PositiveInteger);

    DEFINE_bool(raft_step_down_when_vote_timedout, true,
                "candidate steps down when reaching timeout");
    BRPC_VALIDATE_GFLAG(raft_step_down_when_vote_timedout, brpc::PassValidate);

    DEFINE_bool(raft_enable_append_entries_cache, false,
                "enable cache for out-of-order append entries requests, should used when "
                "pipeline replication is enabled (raft_max_parallel_append_entries_rpc_num > 1).");
    BRPC_VALIDATE_GFLAG(raft_enable_append_entries_cache, ::brpc::PassValidate);

    DEFINE_int32(raft_max_append_entries_cache_size, 8,
                 "the max size of out-of-order append entries cache");
    BRPC_VALIDATE_GFLAG(raft_max_append_entries_cache_size, ::brpc::PositiveInteger);

    DEFINE_int64(raft_append_entry_high_lat_us, 1000 * 1000,
                 "append entry high latency us");
    BRPC_VALIDATE_GFLAG(raft_append_entry_high_lat_us, brpc::PositiveInteger);

    DEFINE_bool(raft_trace_append_entry_latency, false,
                "trace append entry latency");
    BRPC_VALIDATE_GFLAG(raft_trace_append_entry_latency, brpc::PassValidate);

    DECLARE_bool(raft_enable_leader_lease);

#ifndef UNIT_TEST
    static bvar::Adder<int64_t> g_num_nodes("raft_node_count");
#else
    // Unit tests should check this value
    bvar::Adder<int64_t> g_num_nodes("raft_node_count");
#endif

    static bvar::CounterRecorder g_apply_tasks_batch_counter(
        "raft_apply_tasks_batch_counter");

    int SnapshotTimer::adjust_timeout_ms(int timeout_ms)
    {
        if (!_first_schedule)
        {
            return timeout_ms;
        }
        if (timeout_ms > 0)
        {
            timeout_ms = butil::fast_rand_less_than(timeout_ms) + 1;
        }
        _first_schedule = false;
        return timeout_ms;
    }

    class ConfigurationChangeDone : public Closure
    {
    public:
        void Run()
        {
            // 对方节点配置更新操作成功了
            if (status().ok())
            {
                _node->on_configuration_change_done(_term);
                if (_leader_start)
                {
                    // 更新leader的租约
                    _node->leader_lease_start(_lease_epoch);
                    _node->_options.fsm->on_leader_start(_term);
                }
            }
            delete this;
        }

    private:
        ConfigurationChangeDone(
            NodeImpl *node, int64_t term, bool leader_start, int64_t lease_epoch)
            : _node(node), _term(term), _leader_start(leader_start), _lease_epoch(lease_epoch)
        {
            _node->AddRef();
        }
        ~ConfigurationChangeDone()
        {
            _node->Release();
            _node = NULL;
        }
        friend class NodeImpl;
        NodeImpl *_node;
        int64_t _term;
        bool _leader_start;
        int64_t _lease_epoch;
    };

    inline int random_timeout(int timeout_ms)
    {
        int32_t delta = std::min(timeout_ms, FLAGS_raft_max_election_delay_ms);
        return butil::fast_rand_in(timeout_ms, timeout_ms + delta);
    }

    DEFINE_int32(raft_election_heartbeat_factor, 10, "raft election:heartbeat timeout factor");
    static inline int heartbeat_timeout(int election_timeout)
    {
        if (FLAGS_raft_election_heartbeat_factor <= 0)
        {
            LOG(WARNING) << "raft_election_heartbeat_factor flag must be greater than 1"
                         << ", but get " << FLAGS_raft_election_heartbeat_factor
                         << ", it will be set to default value 10.";
            FLAGS_raft_election_heartbeat_factor = 10;
        }
        return std::max(election_timeout / FLAGS_raft_election_heartbeat_factor, 10);
    }

    NodeImpl::NodeImpl(const GroupId &group_id, const PeerId &peer_id)
        : _state(STATE_UNINITIALIZED), _current_term(0), _group_id(group_id), _server_id(peer_id), _conf_ctx(this), _log_storage(NULL), _meta_storage(NULL), _closure_queue(NULL), _config_manager(NULL), _log_manager(NULL), _fsm_caller(NULL), _ballot_box(NULL), _snapshot_executor(NULL), _stop_transfer_arg(NULL), _vote_triggered(false), _waking_candidate(0), _append_entries_cache(NULL), _append_entries_cache_version(0), _node_readonly(false), _majority_nodes_readonly(false)
    {
        butil::string_printf(&_v_group_id, "%s_%d", _group_id.c_str(), _server_id.idx);
        AddRef();
        g_num_nodes << 1;
    }

    NodeImpl::NodeImpl()
        : _state(STATE_UNINITIALIZED), _current_term(0), _group_id(), _server_id(), _conf_ctx(this), _log_storage(NULL), _meta_storage(NULL), _closure_queue(NULL), _config_manager(NULL), _log_manager(NULL), _fsm_caller(NULL), _ballot_box(NULL), _snapshot_executor(NULL), _stop_transfer_arg(NULL), _vote_triggered(false), _waking_candidate(0), _append_entries_cache(NULL), _append_entries_cache_version(0), _node_readonly(false), _majority_nodes_readonly(false)
    {
        butil::string_printf(&_v_group_id, "%s_%d", _group_id.c_str(), _server_id.idx);
        AddRef();
        g_num_nodes << 1;
    }

    NodeImpl::~NodeImpl()
    {
        if (_apply_queue)
        {
            // Wait until no flying task
            _apply_queue->stop();
            _apply_queue.reset();
            bthread::execution_queue_join(_apply_queue_id);
        }

        if (_config_manager)
        {
            delete _config_manager;
            _config_manager = NULL;
        }
        if (_log_manager)
        {
            delete _log_manager;
            _log_manager = NULL;
        }
        if (_fsm_caller)
        {
            delete _fsm_caller;
            _fsm_caller = NULL;
        }
        if (_ballot_box)
        {
            delete _ballot_box;
            _ballot_box = NULL;
        }
        if (_closure_queue)
        {
            delete _closure_queue;
            _closure_queue = NULL;
        }
        if (_options.node_owns_log_storage)
        {
            if (_log_storage)
            {
                delete _log_storage;
                _log_storage = NULL;
            }
        }
        if (_meta_storage)
        {
            delete _meta_storage;
            _meta_storage = NULL;
        }
        if (_snapshot_executor)
        {
            delete _snapshot_executor;
            _snapshot_executor = NULL;
        }
        if (_options.node_owns_fsm)
        {
            _options.node_owns_fsm = false;
            delete _options.fsm;
            _options.fsm = NULL;
        }
        g_num_nodes << -1;
    }

    int NodeImpl::init_snapshot_storage()
    {
        if (_options.snapshot_uri.empty())
        {
            return 0;
        }
        _snapshot_executor = new SnapshotExecutor;
        SnapshotExecutorOptions opt;
        opt.uri = _options.snapshot_uri;
        opt.fsm_caller = _fsm_caller;
        opt.node = this;
        opt.log_manager = _log_manager;
        opt.addr = _server_id.addr;
        opt.init_term = _current_term;
        opt.filter_before_copy_remote = _options.filter_before_copy_remote;
        opt.usercode_in_pthread = _options.usercode_in_pthread;
        if (_options.snapshot_file_system_adaptor)
        {
            opt.file_system_adaptor = *_options.snapshot_file_system_adaptor;
        }
        // get snapshot_throttle
        if (_options.snapshot_throttle)
        {
            opt.snapshot_throttle = *_options.snapshot_throttle;
        }
        return _snapshot_executor->init(opt);
    }

    int NodeImpl::init_log_storage()
    {
        CHECK(_fsm_caller);
        // 创建 _log_storage
        if (_options.log_storage)
        {
            _log_storage = _options.log_storage;
        }
        else
        {
            _log_storage = LogStorage::create(_options.log_uri);
        }
        if (!_log_storage)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " find log storage failed, uri " << _options.log_uri;
            return -1;
        }
        // 初始化 _log_manager
        _log_manager = new LogManager();
        LogManagerOptions log_manager_options;
        log_manager_options.log_storage = _log_storage;
        log_manager_options.configuration_manager = _config_manager;
        log_manager_options.fsm_caller = _fsm_caller;
        return _log_manager->init(log_manager_options);
    }

    int NodeImpl::init_meta_storage()
    {
        // create stable storage
        _meta_storage = RaftMetaStorage::create(_options.raft_meta_uri);
        if (!_meta_storage)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " failed to create meta storage, uri "
                         << _options.raft_meta_uri;
            return ENOENT;
        }

        // check init
        butil::Status status = _meta_storage->init();
        if (!status.ok())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " failed to init meta storage, uri "
                         << _options.raft_meta_uri
                         << ", error " << status;
            return status.error_code();
        }

        // get term and votedfor
        status = _meta_storage->get_term_and_votedfor(&_current_term, &_voted_id, _v_group_id);
        if (!status.ok())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " failed to get term and voted_id when init meta storage,"
                         << " uri " << _options.raft_meta_uri
                         << ", error " << status;
            return status.error_code();
        }

        return 0;
    }

    void NodeImpl::handle_snapshot_timeout()
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // check state
        if (!is_active_state(_state))
        {
            return;
        }

        lck.unlock();
        // TODO: do_snapshot in another thread to avoid blocking the timer thread.
        do_snapshot(NULL);
    }

    int NodeImpl::init_fsm_caller(const LogId &bootstrap_id)
    {
        CHECK(_fsm_caller);
        _closure_queue = new ClosureQueue(_options.usercode_in_pthread);
        // fsm caller init, node AddRef in init
        FSMCallerOptions fsm_caller_options;
        fsm_caller_options.usercode_in_pthread = _options.usercode_in_pthread;
        this->AddRef();
        fsm_caller_options.after_shutdown =
            brpc::NewCallback<NodeImpl *>(after_shutdown, this);
        fsm_caller_options.log_manager = _log_manager;
        // 在这里将fsm_caller和用户自定义状态关联起来
        fsm_caller_options.fsm = _options.fsm;
        fsm_caller_options.closure_queue = _closure_queue;
        fsm_caller_options.node = this;
        fsm_caller_options.bootstrap_id = bootstrap_id;
        const int ret = _fsm_caller->init(fsm_caller_options);
        if (ret != 0)
        {
            delete fsm_caller_options.after_shutdown;
            this->Release();
        }
        return ret;
    }

    class BootstrapStableClosure : public LogManager::StableClosure
    {
    public:
        void Run() { _done.Run(); }
        void wait() { _done.wait(); }

    private:
        SynchronizedClosure _done;
    };

    int NodeImpl::bootstrap(const BootstrapOptions &options)
    {

        if (options.last_log_index > 0)
        {
            if (options.group_conf.empty() || options.fsm == NULL)
            {
                LOG(ERROR) << "Invalid arguments for " << __FUNCTION__
                           << "group_conf=" << options.group_conf
                           << " fsm=" << options.fsm
                           << " while last_log_index="
                           << options.last_log_index;
                return -1;
            }
        }

        if (options.group_conf.empty())
        {
            LOG(ERROR) << "bootstraping an empty node makes no sense";
            return -1;
        }

        // Term is not an option since changing it is very dangerous
        const int64_t boostrap_log_term = options.last_log_index ? 1 : 0;
        const LogId boostrap_id(options.last_log_index, boostrap_log_term);

        _options.fsm = options.fsm;
        _options.node_owns_fsm = options.node_owns_fsm;
        _options.usercode_in_pthread = options.usercode_in_pthread;
        _options.log_uri = options.log_uri;
        _options.raft_meta_uri = options.raft_meta_uri;
        _options.snapshot_uri = options.snapshot_uri;
        _config_manager = new ConfigurationManager();

        // Create _fsm_caller first as log_manager needs it to report error
        _fsm_caller = new FSMCaller();

        if (init_log_storage() != 0)
        {
            LOG(ERROR) << "Fail to init log_storage from " << _options.log_uri;
            return -1;
        }

        if (init_meta_storage() != 0)
        {
            LOG(ERROR) << "Fail to init stable_storage from "
                       << _options.raft_meta_uri;
            return -1;
        }
        if (_current_term == 0)
        {
            _current_term = 1;
            butil::Status status = _meta_storage->set_term_and_votedfor(1, PeerId(), _v_group_id);
            if (!status.ok())
            {
                // TODO add group_id
                LOG(ERROR) << "Fail to set term and votedfor when bootstrap,"
                              " error: "
                           << status;
                return -1;
            }
            return -1;
        }

        if (options.fsm && init_fsm_caller(boostrap_id) != 0)
        {
            LOG(ERROR) << "Fail to init fsm_caller";
            return -1;
        }

        if (options.last_log_index > 0)
        {
            if (init_snapshot_storage() != 0)
            {
                LOG(ERROR) << "Fail to init snapshot_storage from "
                           << _options.snapshot_uri;
                return -1;
            }
            SynchronizedClosure done;
            _snapshot_executor->do_snapshot(&done);
            done.wait();
            if (!done.status().ok())
            {
                LOG(ERROR) << "Fail to save snapshot " << done.status()
                           << " from " << _options.snapshot_uri;
                return -1;
            }
        }
        CHECK_EQ(_log_manager->first_log_index(), options.last_log_index + 1);
        CHECK_EQ(_log_manager->last_log_index(), options.last_log_index);

        LogEntry *entry = new LogEntry();
        entry->AddRef();
        entry->id.term = _current_term;
        entry->type = ENTRY_TYPE_CONFIGURATION;
        entry->peers = new std::vector<PeerId>;
        options.group_conf.list_peers(entry->peers);

        std::vector<LogEntry *> entries;
        entries.push_back(entry);
        BootstrapStableClosure done;
        _log_manager->append_entries(&entries, &done);
        done.wait();
        if (!done.status().ok())
        {
            LOG(ERROR) << "Fail to append configuration";
            return -1;
        }

        return 0;
    }

    int NodeImpl::init(const NodeOptions &options)
    {
        _options = options;

        // check _server_id
        if (butil::IP_ANY == _server_id.addr.ip)
        {
            LOG(ERROR) << "Group " << _group_id
                       << " Node can't started from IP_ANY";
            return -1;
        }

        if (!global_node_manager->server_exists(_server_id.addr))
        {
            LOG(ERROR) << "Group " << _group_id
                       << " No RPC Server attached to " << _server_id.addr
                       << ", did you forget to call braft::add_service()?";
            return -1;
        }

        // 四个定时器:
        // 投票计时器，candidate在此期间没能竞选成功则发起下一轮的选举
        CHECK_EQ(0, _vote_timer.init(this, options.election_timeout_ms + options.max_clock_drift_ms));
        // follower在此时间超时后没有收到leader的消息，就会去调用NodeImpl::handle_election_timeout函数
        CHECK_EQ(0, _election_timer.init(this, options.election_timeout_ms));
        CHECK_EQ(0, _stepdown_timer.init(this, options.election_timeout_ms));
        // 快照相关的计时器，超时后会执行快照
        CHECK_EQ(0, _snapshot_timer.init(this, options.snapshot_interval_s * 1000));

        _config_manager = new ConfigurationManager();

        // 启动线程对_apply_queue_id对应的队列执行execute_applying_tasks函数
        // 该队列与任务提交有关
        if (bthread::execution_queue_start(&_apply_queue_id, NULL,
                                           execute_applying_tasks, this) != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " fail to start execution_queue";
            return -1;
        }

        _apply_queue = execution_queue_address(_apply_queue_id);
        if (!_apply_queue)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " fail to address execution_queue";
            return -1;
        }

        // Create _fsm_caller first as log_manager needs it to report error
        _fsm_caller = new FSMCaller();

        // 初始化租约，用于解决脑裂问题
        _leader_lease.init(options.election_timeout_ms);
        _follower_lease.init(options.election_timeout_ms, options.max_clock_drift_ms);

        // log storage and log manager init
        // 初始化LogStorage, 与日志的持久化有关
        if (init_log_storage() != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " init_log_storage failed";
            return -1;
        }

        // meta init
        // 初始化 RaftMetaStorage ,与raft自身元数据的持久化有关
        if (init_meta_storage() != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " init_meta_storage failed";
            return -1;
        }

        // 初始化FsmCaller, 其用于调用用户自定义的状态
        if (init_fsm_caller(LogId(0, 0)) != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " init_fsm_caller failed";
            return -1;
        }

        // commitment manager init
        // 初始化票箱
        _ballot_box = new BallotBox();
        BallotBoxOptions ballot_box_options;
        ballot_box_options.waiter = _fsm_caller;
        ballot_box_options.closure_queue = _closure_queue;
        if (_ballot_box->init(ballot_box_options) != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " init _ballot_box failed";
            return -1;
        }

        // snapshot storage init and load
        // NOTE: snapshot maybe discard entries when snapshot saved but not discard entries.
        //      init log storage before snapshot storage, snapshot storage will update configration
        // 初始化SnapshotStorage, 与快照的持久化有关
        if (init_snapshot_storage() != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " init_snapshot_storage failed";
            return -1;
        }

        butil::Status st = _log_manager->check_consistency();
        if (!st.ok())
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " is initialized with inconsitency log: "
                       << st;
            return -1;
        }

        _conf.id = LogId();
        // if have log using conf in log, else using conf in options
        // 从这里可以看出，initial_conf只有在这个复制组从空节点启动才会生效，当有snapshot和log里的数据不为空的时候的时候从其中恢复Configuration。
        if (_log_manager->last_log_index() > 0)
        {
            _log_manager->check_and_set_configuration(&_conf);
        }
        else
        {
            _conf.conf = _options.initial_conf;
        }

        // init stable meta and check term
        if (init_meta_storage() != 0)
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " init_meta_storage failed";
            return -1;
        }

        // first start, we can vote directly
        if (_current_term == 1 && _voted_id.is_empty())
        {
            _follower_lease.reset();
        }

        // init replicator
        // 初始化replicator, 与日志复制有关
        ReplicatorGroupOptions rg_options;
        rg_options.heartbeat_timeout_ms = heartbeat_timeout(_options.election_timeout_ms);
        rg_options.election_timeout_ms = _options.election_timeout_ms;
        rg_options.log_manager = _log_manager;
        rg_options.ballot_box = _ballot_box;
        rg_options.node = this;
        rg_options.snapshot_throttle = _options.snapshot_throttle
                                           ? _options.snapshot_throttle->get()
                                           : NULL;
        rg_options.snapshot_storage = _snapshot_executor
                                          ? _snapshot_executor->snapshot_storage()
                                          : NULL;
        _replicator_group.init(NodeId(_group_id, _server_id), rg_options);

        // set state to follower
        // 启动后节点的状态默认为follower状态
        _state = STATE_FOLLOWER;

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " init,"
                  << " term: " << _current_term
                  << " last_log_id: " << _log_manager->last_log_id()
                  << " conf: " << _conf.conf
                  << " old_conf: " << _conf.old_conf;

        // start snapshot timer
        // 启动快照计时器
        if (_snapshot_executor && _options.snapshot_interval_s > 0)
        {
            BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                       << " term " << _current_term << " start snapshot_timer";
            // _snapshot_timer 启动后，每隔一段时间会执行一次 run 方法, 在run方法中执行  handle_snapshot_timeout
            _snapshot_timer.start();
        }

        if (!_conf.empty())
        {
            step_down(_current_term, false, butil::Status::OK());
        }

        // add node to NodeManager
        //  将当前节点交给NodeManager管理，其内部会维护所有的Node
        if (!global_node_manager->add(this))
        {
            LOG(ERROR) << "NodeManager add " << _group_id
                       << ":" << _server_id << " failed";
            return -1;
        }

        // Now the raft node is started , have to acquire the lock to avoid race
        // conditions
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // 如果_conf里面只有一个自己一个server，则调用elect_self将自己设置为leader
        if (_conf.stable() && _conf.conf.size() == 1u && _conf.conf.contains(_server_id))
        {
            // The group contains only this server which must be the LEADER, trigger
            // the timer immediately.
            elect_self(&lck);
        }

        return 0;
    }

    DEFINE_int32(raft_apply_batch, 32, "Max number of tasks that can be applied "
                                       " in a single batch");
    BRPC_VALIDATE_GFLAG(raft_apply_batch, ::brpc::PositiveInteger);

    int NodeImpl::execute_applying_tasks(
        void *meta, bthread::TaskIterator<LogEntryAndClosure> &iter)
    {
        if (iter.is_queue_stopped())
        {
            return 0;
        }
        // TODO: the batch size should limited by both task size and the total log
        // size
        const size_t batch_size = FLAGS_raft_apply_batch;
        DEFINE_SMALL_ARRAY(LogEntryAndClosure, tasks, batch_size, 256);
        size_t cur_size = 0;
        NodeImpl *m = (NodeImpl *)meta;
        // 每batch_size个task进行一次批量apply
        for (; iter; ++iter)
        {
            if (cur_size == batch_size)
            {
                m->apply(tasks, cur_size);
                cur_size = 0;
            }
            tasks[cur_size++] = *iter;
        }
        // 不足batch_size数量的task进行一次批量apply
        if (cur_size > 0)
        {
            m->apply(tasks, cur_size);
        }
        return 0;
    }

    void NodeImpl::apply(const Task &task)
    {
        // 构造LogEntry
        LogEntry *entry = new LogEntry;
        entry->AddRef();
        entry->data.swap(*task.data);
        LogEntryAndClosure m;
        m.entry = entry;
        m.done = task.done;
        m.expected_term = task.expected_term;
        // 将任务放到队列执行
        if (_apply_queue->execute(m, &bthread::TASK_OPTIONS_INPLACE, NULL) != 0)
        {
            task.done->status().set_error(EPERM, "Node is down");
            entry->Release();
            return run_closure_in_bthread(task.done);
        }
    }

    void NodeImpl::on_configuration_change_done(int64_t term)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_state > STATE_TRANSFERRING || term != _current_term)
        {
            LOG(WARNING) << "node " << node_id()
                         << " process on_configuration_change_done "
                         << " at term=" << term
                         << " while state=" << state2str(_state)
                         << " and current_term=" << _current_term;
            // Callback from older version
            return;
        }
        // 当前的配置状态是 STAGE_JOINT, 要转换为STAGE_STABLE
        _conf_ctx.next_stage();
    }

    class OnCaughtUp : public CatchupClosure
    {
    public:
        OnCaughtUp(NodeImpl *node, int64_t term, const PeerId &peer, int64_t version)
            : _node(node), _term(term), _peer(peer), _version(version)
        {
            _node->AddRef();
        }
        ~OnCaughtUp()
        {
            if (_node)
            {
                _node->Release();
                _node = NULL;
            }
        }
        virtual void Run()
        {
            _node->on_caughtup(_peer, _term, _version, status());
            delete this;
        };

    private:
        NodeImpl *_node;
        int64_t _term;
        PeerId _peer;
        int64_t _version;
    };

    void NodeImpl::on_caughtup(const PeerId &peer, int64_t term,
                               int64_t version, const butil::Status &st)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        // CHECK _state and _current_term to avoid ABA problem
        if (_state != STATE_LEADER || term != _current_term)
        {
            // if leader stepped down, reset() has already been called in step_down(),
            // so nothing needs to be done here
            LOG(WARNING) << "node " << node_id() << " stepped down when waiting peer "
                         << peer << " to catch up, current state is " << state2str(_state)
                         << ", current term is " << _current_term
                         << ", expect term is " << term;
            return;
        }

        //  追赶过程成功了
        if (st.ok())
        { // Caught up successfully
            _conf_ctx.on_caughtup(version, peer, true);
            return;
        }

        // 失败了就重试，前提是节点得活着
        // Retry if this peer is still alive
        if (st.error_code() == ETIMEDOUT && (butil::monotonic_time_ms() - _replicator_group.last_rpc_send_timestamp(peer)) <= _options.election_timeout_ms)
        {

            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " waits peer " << peer << " to catch up";

            OnCaughtUp *caught_up = new OnCaughtUp(this, _current_term, peer, version);
            timespec due_time = butil::milliseconds_from_now(
                _options.get_catchup_timeout_ms());

            //  #####
            if (0 == _replicator_group.wait_caughtup(
                         peer, _options.catchup_margin, &due_time, caught_up))
            {
                return;
            }
            else
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " wait_caughtup failed, peer " << peer;
                delete caught_up;
            }
        }

        _conf_ctx.on_caughtup(version, peer, false);
    }

    void NodeImpl::check_dead_nodes(const Configuration &conf, int64_t now_ms)
    {
        // 获取当前配置下所有的节点
        std::vector<PeerId> peers;
        conf.list_peers(&peers);
        // 存活的节点数
        size_t alive_count = 0;
        Configuration dead_nodes; // for easily print
        for (size_t i = 0; i < peers.size(); i++)
        {
            // 当前节点必然是存活的
            if (peers[i] == _server_id)
            {
                ++alive_count;
                continue;
            }

            // 检查时间距离最近一次向其发送rpc消息的时间间隔修小于election_timeout_ms，则该节点还活着
            // ????last_rpc_send_timestamp是leader最近一次给follower发送消息的时间还是follower最近一次响应的时间????
            if (now_ms - _replicator_group.last_rpc_send_timestamp(peers[i]) <= _options.election_timeout_ms)
            {
                ++alive_count;
                continue;
            }
            // 否则标记为失联节点
            dead_nodes.add_peer(peers[i]);
        }
        // 失联节点数没有达到半数则leader还可以正常工作
        if (alive_count >= peers.size() / 2 + 1)
        {
            return;
        }
        LOG(WARNING) << "node " << node_id()
                     << " term " << _current_term
                     << " steps down when alive nodes don't satisfy quorum"
                        " dead_nodes: "
                     << dead_nodes
                     << " conf: " << conf;
        butil::Status status;
        // 失联节点达到了半数，可能当前leader出现在了小分区，为了防止出现脑裂需要退回到follower状态
        status.set_error(ERAFTTIMEDOUT, "Majority of the group dies");
        step_down(_current_term, false, status);
    }

    void NodeImpl::handle_stepdown_timeout()
    {
        BAIDU_SCOPED_LOCK(_mutex);

        // check state
        // STATE_LEADER和STATE_TRANSFERRING以外的状态不用超时处理
        if (_state > STATE_TRANSFERRING)
        {
            BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                       << " term " << _current_term << " stop stepdown_timer"
                       << " state is " << state2str(_state);
            return;
        }

        int64_t now = butil::monotonic_time_ms();
        // 检查新配置下失联的节点
        check_dead_nodes(_conf.conf, now);
        if (!_conf.old_conf.empty())
        {
            check_dead_nodes(_conf.old_conf, now);
        }
    }

    void NodeImpl::unsafe_register_conf_change(const Configuration &old_conf,
                                               const Configuration &new_conf,
                                               Closure *done)
    {
        // 如果不是leader则无法更改配置
        if (_state != STATE_LEADER)
        {
            LOG(WARNING) << "[" << node_id()
                         << "] Refusing configuration changing because the state is "
                         << state2str(_state);
            if (done)
            {
                // 这两种情况虽然执行了回调函数，但是在回调函数开始判断状态时就无法通过
                if (_state == STATE_TRANSFERRING)
                {
                    done->status().set_error(EBUSY, "Is transferring leadership");
                }
                else
                {
                    done->status().set_error(EPERM, "Not leader");
                }
                run_closure_in_bthread(done);
            }
            return;
        }

        // check concurrent conf change

        // 当前节点是leader，但是_conf_ctx处于忙碌状态，表示有另一个配置变更在发生
        // 一共有四个配置变更相关的状态
        // STAGE_NONE = 0,
        // STAGE_CATCHING_UP = 1, // 追赶状态, 如果有新的节点加入就需要追赶之后再配置变更
        // STAGE_JOINT = 2,   // 一致状态，新旧配置共同起作用
        // STAGE_STABLE = 3,

        // 这里的判断条件是state不是STAGE_NONE状态，也就是说只有 _stage == STAGE_NONE 时才能进行配置变更
        if (_conf_ctx.is_busy())
        {
            LOG(WARNING) << "[" << node_id()
                         << " ] Refusing concurrent configuration changing";
            if (done)
            {
                done->status().set_error(EBUSY, "Doing another configuration change");
                run_closure_in_bthread(done);
            }
            return;
        }

        // Return immediately when the new peers equals to current configuration
        // 新配置和现有配置一样，不用修改
        if (_conf.conf.equals(new_conf))
        {
            run_closure_in_bthread(done);
            return;
        }

        // 如果不满足上述条件则开始更改配置
        return _conf_ctx.start(old_conf, new_conf, done);
    }

    butil::Status NodeImpl::list_peers(std::vector<PeerId> *peers)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_state != STATE_LEADER)
        {
            return butil::Status(EPERM, "Not leader");
        }
        _conf.conf.list_peers(peers);
        return butil::Status::OK();
    }

    void NodeImpl::add_peer(const PeerId &peer, Closure *done)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        Configuration new_conf = _conf.conf;
        new_conf.add_peer(peer);
        return unsafe_register_conf_change(_conf.conf, new_conf, done);
    }

    void NodeImpl::remove_peer(const PeerId &peer, Closure *done)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        Configuration new_conf = _conf.conf;
        new_conf.remove_peer(peer);
        return unsafe_register_conf_change(_conf.conf, new_conf, done);
    }

    void NodeImpl::change_peers(const Configuration &new_peers, Closure *done)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        return unsafe_register_conf_change(_conf.conf, new_peers, done);
    }

    butil::Status NodeImpl::reset_peers(const Configuration &new_peers)
    {
        BAIDU_SCOPED_LOCK(_mutex);

        if (new_peers.empty())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id << " set empty peers";
            return butil::Status(EINVAL, "new_peers is empty");
        }
        // check state
        if (!is_active_state(_state))
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " is in state " << state2str(_state) << ", can't reset_peer";
            return butil::Status(EPERM, "Bad state %s", state2str(_state));
        }
        // check bootstrap
        if (_conf.conf.empty())
        {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " reset_peers to " << new_peers << " from empty";
            _conf.conf = new_peers;
            butil::Status status;
            status.set_error(ESETPEER, "Set peer from empty configuration");
            step_down(_current_term + 1, false, status);
            return butil::Status::OK();
        }

        // check concurrent conf change
        if (_state == STATE_LEADER && _conf_ctx.is_busy())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " reset_peer need wait current conf change";
            return butil::Status(EBUSY, "Changing to another configuration");
        }

        // check equal, maybe retry direct return
        if (_conf.conf.equals(new_peers))
        {
            return butil::Status::OK();
        }

        Configuration new_conf(new_peers);
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " set_peer from "
                     << _conf.conf << " to " << new_conf;
        // change conf and step_down
        _conf.conf = new_conf;
        _conf.old_conf.reset();
        butil::Status status;
        status.set_error(ESETPEER, "Raft node set peer normally");
        step_down(_current_term + 1, false, status);
        return butil::Status::OK();
    }

    void NodeImpl::snapshot(Closure *done)
    {
        do_snapshot(done);
    }

    void NodeImpl::do_snapshot(Closure *done)
    {
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " starts to do snapshot";
        if (_snapshot_executor)
        {
            _snapshot_executor->do_snapshot(done);
        }
        // 打快照只能另起线程执行
        else
        {
            if (done)
            {
                done->status().set_error(EINVAL, "Snapshot is not supported");
                run_closure_in_bthread(done);
            }
        }
    }

    void NodeImpl::shutdown(Closure *done)
    {
        // Note: shutdown is probably invoked more than once, make sure this method
        // is idempotent
        {
            BAIDU_SCOPED_LOCK(_mutex);

            LOG(INFO) << "node " << _group_id << ":" << _server_id << " shutdown,"
                                                                      " current_term "
                      << _current_term << " state " << state2str(_state);

            if (_state < STATE_SHUTTING)
            { // Got the right to shut
                // Remove node from NodeManager and |this| would not be accessed by
                // the coming RPCs
                global_node_manager->remove(this);
                // if it is leader, set the wakeup_a_candidate with true,
                // if it is follower, call on_stop_following in step_down
                if (_state <= STATE_FOLLOWER)
                {
                    butil::Status status;
                    status.set_error(ESHUTDOWN, "Raft node is going to quit.");
                    step_down(_current_term, _state == STATE_LEADER, status);
                }

                // change state to shutdown
                _state = STATE_SHUTTING;

                // Destroy all the timer
                _election_timer.destroy();
                _vote_timer.destroy();
                _stepdown_timer.destroy();
                _snapshot_timer.destroy();

                // stop replicator and fsm_caller wait
                if (_log_manager)
                {
                    _log_manager->shutdown();
                }

                if (_snapshot_executor)
                {
                    _snapshot_executor->shutdown();
                }

                // step_down will call _commitment_manager->clear_pending_applications(),
                // this can avoid send LogEntry with closure to fsm_caller.
                // fsm_caller shutdown will not leak user's closure.
                if (_fsm_caller)
                {
                    _fsm_caller->shutdown();
                }
            }

            if (_state != STATE_SHUTDOWN)
            {
                // This node is shutting, push done into the _shutdown_continuations
                // and after_shutdown would invoked this callbacks.
                if (done)
                {
                    _shutdown_continuations.push_back(done);
                }
                return;
            }
        } // out of _mutex;

        // This node is down, it's ok to invoke done right now. Don't inovke this
        // inplace to avoid the dead lock issue when done->Run() is going to acquire
        // a mutex which is already held by the caller
        if (done)
        {
            run_closure_in_bthread(done);
        }
    }

    void NodeImpl::join()
    {
        if (_fsm_caller)
        {
            _fsm_caller->join();
        }
        if (_snapshot_executor)
        {
            _snapshot_executor->join();
        }
        // We have to join the _waking_candidate which is sending TimeoutNowRequest,
        // otherwise the process is likely going to quit and the RPC would be stop
        // as the working threads are stopped as well
        Replicator::join(_waking_candidate);
    }

    // 处理选举超时
    void NodeImpl::handle_election_timeout()
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // check state
        if (_state != STATE_FOLLOWER)
        {
            return;
        }

        // Trigger vote manually, or wait until follower lease expire.
        // 这里还需要根据租约进行判断，即使当前follower长时间没有收到leader的消息，导致_election_timer到期了
        // 如果租约还没到期，也不会发起pre_vote的，这样就避免了脑裂的发生
        if (!_vote_triggered && !_follower_lease.expired())
        {

            return;
        }
        bool triggered = _vote_triggered;
        _vote_triggered = false;

        // Reset leader as the leader is uncerntain on election timeout.
        PeerId empty_id;
        butil::Status status;
        status.set_error(ERAFTTIMEDOUT, "Lost connection from leader %s",
                         _leader_id.to_string().c_str());
        // 重置当前节点的leader为空
        reset_leader_id(empty_id, status);
        // 发起预投票
        return pre_vote(&lck, triggered);
        // Don't touch any thing of *this ever after
    }

    void NodeImpl::handle_timeout_now_request(brpc::Controller *controller,
                                              const TimeoutNowRequest *request,
                                              TimeoutNowResponse *response,
                                              google::protobuf::Closure *done)
    {
        brpc::ClosureGuard done_guard(done);
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // 目标节点的term必须和leader的term相等才行，因为原leader下线是通过向其返回更大的term进行的
        if (request->term() != _current_term)
        {
            const int64_t saved_current_term = _current_term;
            if (request->term() > _current_term)
            {
                butil::Status status;
                status.set_error(EHIGHERTERMREQUEST, "Raft node receives higher term request.");
                step_down(request->term(), false, status);
            }
            response->set_term(_current_term);
            response->set_success(false);
            lck.unlock();
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " received handle_timeout_now_request "
                         "while _current_term="
                      << saved_current_term << " didn't match request_term="
                      << request->term();
            return;
        }

        if (_state != STATE_FOLLOWER)
        {
            const State saved_state = _state;
            const int64_t saved_term = _current_term;
            response->set_term(_current_term);
            response->set_success(false);
            lck.unlock();
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " received handle_timeout_now_request "
                         "while state is "
                      << state2str(saved_state)
                      << " at term=" << saved_term;
            return;
        }
        const butil::EndPoint remote_side = controller->remote_side();
        const int64_t saved_term = _current_term;
        if (FLAGS_raft_enable_leader_lease)
        {
            // We will disrupt the leader, don't let the old leader
            // step down.
            response->set_term(_current_term);
        }
        else
        {
            // 向leader响应更大term使其step_down
            response->set_term(_current_term + 1);
        }
        response->set_success(true);
        // Parallelize Response and election
        run_closure_in_bthread(done_guard.release());

        // 直接发起正式投票请求
        elect_self(&lck);
        // Don't touch any mutable field after this point, it's likely out of the
        // critical section
        if (lck.owns_lock())
        {
            lck.unlock();
        }
        // Note: don't touch controller, request, response, done anymore since they
        // were dereferenced at this point
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received handle_timeout_now_request from "
                  << remote_side << " at term=" << saved_term;
    }

    class StopTransferArg
    {
        DISALLOW_COPY_AND_ASSIGN(StopTransferArg);

    public:
        StopTransferArg(NodeImpl *n, int64_t t, const PeerId &p)
            : node(n), term(t), peer(p)
        {
            node->AddRef();
        }
        ~StopTransferArg()
        {
            node->Release();
        }
        NodeImpl *node;
        int64_t term;
        PeerId peer;
    };

    void on_transfer_timeout(void *arg)
    {
        StopTransferArg *a = (StopTransferArg *)arg;
        a->node->handle_transfer_timeout(a->term, a->peer);
        delete a;
    }

    // leader转让领导权超时后的处理
    void NodeImpl::handle_transfer_timeout(int64_t term, const PeerId &peer)
    {
        LOG(INFO) << "node " << node_id() << " failed to transfer leadership to peer="
                  << peer << " : reached timeout";
        BAIDU_SCOPED_LOCK(_mutex);
        if (term == _current_term)
        {
            _replicator_group.stop_transfer_leadership(peer);
            if (_state == STATE_TRANSFERRING)
            {
                _leader_lease.on_leader_start(term);
                _fsm_caller->on_leader_start(term, _leader_lease.lease_epoch());
                // 将状态改回来
                _state = STATE_LEADER;
                _stop_transfer_arg = NULL;
            }
        }
    }

    int NodeImpl::transfer_leadership_to(const PeerId &peer)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        if (_state != STATE_LEADER)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " is in state " << state2str(_state);
            return _state == STATE_TRANSFERRING ? EBUSY : EPERM;
        }
        if (_conf_ctx.is_busy() /*FIXME: make this expression more readable*/)
        {
            // It's very messy to deal with the case when the |peer| received
            // TimeoutNowRequest and increase the term while somehow another leader
            // which was not replicated with the newest configuration has been
            // elected. If no add_peer with this very |peer| is to be invoked ever
            // after nor this peer is to be killed, this peer will spin in the voting
            // procedure and make the each new leader stepped down when the peer
            // reached vote timedout and it starts to vote (because it will increase
            // the term of the group)
            // To make things simple, refuse the operation and force users to
            // invoke transfer_leadership_to after configuration changing is
            // completed so that the peer's configuration is up-to-date when it
            // receives the TimeOutNowRequest.
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " refused to transfer leadership to peer " << peer
                         << " when the leader is changing the configuration";
            return EBUSY;
        }

        PeerId peer_id = peer;

        // 没有指定目标peer节点
        if (peer_id == ANY_PEER)
        {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " starts to transfer leadership to any peer.";
            // 从 _replicator_group 中找一个next_index最大的节点作为目标节点
            if (_replicator_group.find_the_next_candidate(&peer_id, _conf) != 0)
            {
                return -1;
            }
        }
        // 目标节点是自身就不用继续操作了
        if (peer_id == _server_id)
        {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " transfering leadership to self";
            return 0;
        }
        // 目标节点不在配置的节点集合中，显然这个操作是不合法的
        if (!_conf.contains(peer_id))
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " refused to transfer leadership to peer " << peer_id
                         << " which doesn't belong to " << _conf.conf;
            return EINVAL;
        }

        // 一切正常，可以正常进行 transfer
        const int64_t last_log_index = _log_manager->last_log_index();
        const int rc = _replicator_group.transfer_leadership_to(peer_id, last_log_index);
        if (rc != 0)
        {
            if (rc == EINVAL)
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " fail to transfer leadership, no such peer=" << peer_id;
            }
            else if (rc == EHOSTUNREACH)
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " fail to transfer leadership, peer=" << peer_id
                             << " whose consecutive_error_times not 0.";
            }
            else
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " fail to transfer leadership, peer=" << peer_id
                             << " err: " << berror(rc);
            }
            return rc;
        }
        // leader设置自身状态为STATE_TRANSFERRING表示正在进行状态交接
        _state = STATE_TRANSFERRING;
        butil::Status status;
        status.set_error(ETRANSFERLEADERSHIP, "Raft leader is transferring "
                                              "leadership to %s",
                         peer_id.to_string().c_str());
        // 调用状态机的on_leader_stop
        _leader_lease.on_leader_stop();
        _fsm_caller->on_leader_stop(status);
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " starts to transfer leadership to " << peer_id;
        _stop_transfer_arg = new StopTransferArg(this, _current_term, peer_id);
        // 开启_transfer_timer计时器，如果超时还没有transfer成功，就调用NodeImpl::handle_transfer_timeout停止transfer，并将_state设置回leader
        if (bthread_timer_add(&_transfer_timer,
                              butil::milliseconds_from_now(_options.election_timeout_ms),
                              on_transfer_timeout, _stop_transfer_arg) != 0)
        {
            lck.unlock();
            LOG(ERROR) << "Fail to add timer";
            on_transfer_timeout(_stop_transfer_arg);
            return -1;
        }
        return 0;
    }

    butil::Status NodeImpl::vote(int election_timeout_ms)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        if (_state != STATE_FOLLOWER)
        {
            return butil::Status(EPERM, "is not follower");
        }
        // 选举超时
        int max_election_timeout_ms = _options.max_clock_drift_ms + _options.election_timeout_ms;
        if (election_timeout_ms > max_election_timeout_ms)
        {
            return butil::Status(EINVAL, "election_timeout_ms larger than safety threshold");
        }
        election_timeout_ms = std::min(election_timeout_ms, max_election_timeout_ms);
        int max_clock_drift_ms = max_election_timeout_ms - election_timeout_ms;
        unsafe_reset_election_timeout_ms(election_timeout_ms, max_clock_drift_ms);
        _vote_triggered = true;
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " trigger-vote,"
                                                                  " current_term "
                  << saved_current_term << " state " << state2str(saved_state) << " election_timeout " << election_timeout_ms;
        return butil::Status();
    }

    butil::Status NodeImpl::reset_election_timeout_ms(int election_timeout_ms)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        int max_election_timeout_ms = _options.max_clock_drift_ms + _options.election_timeout_ms;
        if (election_timeout_ms > max_election_timeout_ms)
        {
            return butil::Status(EINVAL, "election_timeout_ms larger than safety threshold");
        }
        election_timeout_ms = std::min(election_timeout_ms, max_election_timeout_ms);
        int max_clock_drift_ms = max_election_timeout_ms - election_timeout_ms;
        unsafe_reset_election_timeout_ms(election_timeout_ms, max_clock_drift_ms);
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " reset_election_timeout,"
                                                                  " current_term "
                  << saved_current_term << " state " << state2str(saved_state) << " new election_timeout " << election_timeout_ms << " new clock_drift_ms " << max_clock_drift_ms;
        return butil::Status();
    }

    void NodeImpl::reset_election_timeout_ms(int election_timeout_ms,
                                             int max_clock_drift_ms)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        unsafe_reset_election_timeout_ms(election_timeout_ms, max_clock_drift_ms);
        const int64_t saved_current_term = _current_term;
        const State saved_state = _state;
        lck.unlock();

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " reset_election_timeout,"
                                                                  " current_term "
                  << saved_current_term << " state " << state2str(saved_state) << " new election_timeout " << election_timeout_ms << " new clock_drift_ms " << max_clock_drift_ms;
    }

    void NodeImpl::unsafe_reset_election_timeout_ms(int election_timeout_ms,
                                                    int max_clock_drift_ms)
    {
        _options.election_timeout_ms = election_timeout_ms;
        _options.max_clock_drift_ms = max_clock_drift_ms;
        _replicator_group.reset_heartbeat_interval(
            heartbeat_timeout(_options.election_timeout_ms));
        _replicator_group.reset_election_timeout_interval(_options.election_timeout_ms);
        _election_timer.reset(election_timeout_ms);
        _leader_lease.reset_election_timeout_ms(election_timeout_ms);
        _follower_lease.reset_election_timeout_ms(election_timeout_ms, _options.max_clock_drift_ms);
    }

    void NodeImpl::on_error(const Error &e)
    {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
                     << " got error=" << e;
        if (_fsm_caller)
        {
            // on_error of _fsm_caller is guaranteed to be executed once.
            _fsm_caller->on_error(e);
        }
        std::unique_lock<raft_mutex_t> lck(_mutex);
        // if it is leader, need to wake up a new one.
        // if it is follower, also step down to call on_stop_following
        if (_state <= STATE_FOLLOWER)
        {
            butil::Status status;
            status.set_error(EBADNODE, "Raft node(leader or candidate) is in error.");
            step_down(_current_term, _state == STATE_LEADER, status);
        }
        if (_state < STATE_ERROR)
        {
            _state = STATE_ERROR;
        }
        lck.unlock();
    }

    void NodeImpl::handle_vote_timeout()
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // check state
        if (_state != STATE_CANDIDATE)
        {
            return;
        }
        // 如果 _vote_timer 触发时节点还处于candidate状态，也就是还没有选出leader，则需要进行超时处理: 将状态改回follower
        // 然后发起新一轮的预投票
        // 这个参数默认是开启的
        if (FLAGS_raft_step_down_when_vote_timedout)
        {
            // step down to follower
            LOG(WARNING) << "node " << node_id()
                         << " term " << _current_term
                         << " steps down when reaching vote timeout:"
                            " fail to get quorum vote-granted";
            butil::Status status;
            status.set_error(ERAFTTIMEDOUT, "Fail to get quorum vote-granted");
            step_down(_current_term, false, status);
            pre_vote(&lck, false);
        }
        // 否则如果不开启这个参数，就会重新发起正式投票
        else
        {
            // retry vote
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " term " << _current_term << " retry elect";
            elect_self(&lck);
        }
    }

    // 节点收到requestvote后的响应
    void NodeImpl::handle_request_vote_response(const PeerId &peer_id, const int64_t term,
                                                const int64_t ctx_version,
                                                const RequestVoteResponse &response)
    {
        BAIDU_SCOPED_LOCK(_mutex);

        if (ctx_version != _vote_ctx.version())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received invalid RequestVoteResponse from " << peer_id
                         << " ctx_version " << ctx_version
                         << " current_ctx_version " << _vote_ctx.version();
            return;
        }

        // check state
        // 正式投票请求只有candidate才能发起，如果不是candidate说明可能已经成leader了
        if (_state != STATE_CANDIDATE)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received invalid RequestVoteResponse from " << peer_id
                         << " state not in CANDIDATE but " << state2str(_state);
            return;
        }
        // check stale response
        // 防止过期的RequestVoteRpc的响应
        if (term != _current_term)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received stale RequestVoteResponse from " << peer_id
                         << " term " << term << " current_term " << _current_term;
            return;
        }
        // check response term
        // 如果对方的term大于自身的term，则回退到follower状态
        if (response.term() > _current_term)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received invalid RequestVoteResponse from " << peer_id
                         << " term " << response.term() << " expect " << _current_term;
            butil::Status status;
            status.set_error(EHIGHERTERMRESPONSE, "Raft node receives higher term "
                                                  "request_vote_response.");
            step_down(response.term(), false, status);
            return;
        }

        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received RequestVoteResponse from " << peer_id
                  << " term " << response.term() << " granted " << response.granted()
                  << " rejected_by_lease " << response.rejected_by_lease()
                  << " disrupted " << response.disrupted();

        // 租约到期但是没有收到选票
        if (!response.granted() && !response.rejected_by_lease())
        {
            return;
        }

        // 对方是 leader
        if (response.disrupted())
        {
            _vote_ctx.set_disrupted_leader(DisruptedLeader(peer_id, response.previous_term()));
        }
        // 运行到这里说明租约没到期或者没收到选票，分开判断:
        // 1. 获得了该节点的投票
        if (response.granted())
        {
            // 法定票数减1
            _vote_ctx.grant(peer_id);
            //
            if (peer_id == _follower_lease.last_leader())
            {
                _vote_ctx.grant(_server_id);
                _vote_ctx.stop_grant_self_timer(this);
            }
            // 每次收到一个节点的投票后法定票数减1，减到0后说明收到了过半数节点的投票
            // 这时候granted()就返回true，就可以become_leader了
            if (_vote_ctx.granted())
            {
                return become_leader();
            }
        }
        // 2. 租约没到期，保留该节点稍后重试
        else
        {
            // If the follower rejected the vote because of lease, reserve it, and
            // the candidate will try again after it disrupt the old leader.
            _vote_ctx.reserve(peer_id);
        }
        // 重新向该节点发起投票请求
        retry_vote_on_reserved_peers();
    }

    void NodeImpl::retry_vote_on_reserved_peers()
    {
        std::set<PeerId> peers;
        _vote_ctx.pop_grantable_peers(&peers);
        if (peers.empty())
        {
            return;
        }
        request_peers_to_vote(peers, &_vote_ctx.disrupted_leader());
    }

    struct OnRequestVoteRPCDone : public google::protobuf::Closure
    {
        OnRequestVoteRPCDone(const PeerId &peer_id_, const int64_t term_,
                             const int64_t ctx_version_, NodeImpl *node_)
            : peer(peer_id_), term(term_), ctx_version(ctx_version_), node(node_)
        {
            node->AddRef();
        }
        virtual ~OnRequestVoteRPCDone()
        {
            node->Release();
        }

        void Run()
        {
            do
            {
                if (cntl.ErrorCode() != 0)
                {
                    LOG(WARNING) << "node " << node->node_id()
                                 << " received RequestVoteResponse from " << peer
                                 << " error: " << cntl.ErrorText();
                    break;
                }
                // 收到其他节点对投票请求的响应后的实际处理函数
                node->handle_request_vote_response(peer, term, ctx_version, response);
            } while (0);
            delete this;
        }

        PeerId peer;
        int64_t term;
        int64_t ctx_version;
        RequestVoteRequest request;
        RequestVoteResponse response;
        brpc::Controller cntl;
        NodeImpl *node;
    };

    // 节点收到其他节点对preVote rpc请求的响应后实际的处理逻辑
    void NodeImpl::handle_pre_vote_response(const PeerId &peer_id, const int64_t term,
                                            const int64_t ctx_version,
                                            const RequestVoteResponse &response, const brpc::Controller &cntl)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        if (ctx_version != _pre_vote_ctx.version())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received invalid PreVoteResponse from " << peer_id
                         << " ctx_version " << ctx_version
                         << " current_ctx_version " << _pre_vote_ctx.version();
            return;
        }

        // check state
        // pre_vote请求是在follower状态下发起的
        if (_state != STATE_FOLLOWER)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received invalid PreVoteResponse from " << peer_id
                         << " state not in STATE_FOLLOWER but " << state2str(_state);
            return;
        }
        // check stale response
        // term是follower发起pre_vote时的term，current是当前的term
        // 不相等说明在发起pre_vote之后有其他节点成为leader更新了当前follower的term
        if (term != _current_term)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received stale PreVoteResponse from " << peer_id
                         << " term " << term << " current_term " << _current_term;
            return;
        }

        ////////////////////
        ////////////////////
        butil::IOBuf data_buf;
        butil::IOBuf response_buf = cntl.response_attachment();
        data_buf.swap(response_buf);
        std::vector<LogEntry *>
            entries;
        entries.reserve(response.entries_size());
        for (int i = 0; i < response.entries_size(); i++)
        {
            const EntryMeta &entry = response.entries(i);
            if (entry.type() != ENTRY_TYPE_UNKNOWN)
            {
                LogEntry *log_entry = new LogEntry();
                log_entry->AddRef();
                log_entry->id.term = entry.term();
                log_entry->id.index = entry.index();
                log_entry->type = (EntryType)entry.type();
                if (entry.peers_size() > 0)
                {
                    log_entry->peers = new std::vector<PeerId>;
                    for (int i = 0; i < entry.peers_size(); i++)
                    {
                        log_entry->peers->push_back(entry.peers(i));
                    }
                    CHECK_EQ(log_entry->type, ENTRY_TYPE_CONFIGURATION);
                    if (entry.old_peers_size() > 0)
                    {
                        log_entry->old_peers = new std::vector<PeerId>;
                        for (int i = 0; i < entry.old_peers_size(); i++)
                        {
                            log_entry->old_peers->push_back(entry.old_peers(i));
                        }
                    }
                }
                else
                {
                    CHECK_NE(entry.type(), ENTRY_TYPE_CONFIGURATION);
                }
                if (entry.has_data_len())
                {
                    int len = entry.data_len();
                    data_buf.cutn(&log_entry->data, len);
                }
                entries.push_back(log_entry);
            }
        }

        // 收到其他节点补充的entry之后需要将这些entry按位置放回到自己的entry数组中
        // !!!待完成
        // 需要了解logs_in_memory的结构, 和append_entry有关。

        ////////////////////
        ////////////////////
        // check response term
        // 检查response中的term，如果大于自身的term，则执行 step_down 并更新 term 值。
        if (response.term() > _current_term)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received invalid PreVoteResponse from " << peer_id
                         << " term " << response.term() << " expect " << _current_term;
            butil::Status status;
            status.set_error(EHIGHERTERMRESPONSE, "Raft node receives higher term "
                                                  "pre_vote_response.");
            step_down(response.term(), false, status);
            return;
        }

        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received PreVoteResponse from " << peer_id
                  << " term " << response.term() << " granted " << response.granted()
                  << " rejected_by_lease " << response.rejected_by_lease()
                  << " disrupted " << response.disrupted();

        // 租约已经过期但是没有收到选票
        if (!response.granted() && !response.rejected_by_lease())
        {
            return;
        }

        // Internal vote should respect lease.
        // 租期还没到
        if (response.rejected_by_lease() && !_pre_vote_ctx.triggered())
        {
            return;
        }

        // 对方是leader
        if (response.disrupted())
        {
            _pre_vote_ctx.set_disrupted_leader(DisruptedLeader(peer_id, response.previous_term()));
        }

        // peers保存投票的节点
        std::set<PeerId> peers;
        if (response.rejected_by_lease())
        {
            // Temporarily reserve the vote of follower because the lease is
            // still valid. Until we make sure the leader can be disrupted,
            // the vote can't be counted.
            _pre_vote_ctx.reserve(peer_id);
            _pre_vote_ctx.pop_grantable_peers(&peers);
        }
        else
        {
            _pre_vote_ctx.pop_grantable_peers(&peers);
            peers.insert(peer_id);
        }

        // 运行到此处说明收到了选票, 更新_pre_vote_ctx,
        // _pre_vote_ctx中存储着当前的投票情况，它的成员_quorum初始化为peer数量的一半加一，每次被grant就会减一
        for (std::set<PeerId>::const_iterator it = peers.begin();
             it != peers.end(); ++it)
        {
            // 将法定票数减1
            _pre_vote_ctx.grant(*it);
            if (*it == _follower_lease.last_leader())
            {
                _pre_vote_ctx.grant(_server_id);
                _pre_vote_ctx.stop_grant_self_timer(this);
            }
        }
        // _pre_vote_ctx.granted是检查_quorum是否小于等于0, 是则说明获得了大多数的选票，返回true
        if (_pre_vote_ctx.granted())
        {
            // 调用elect_self开始选举
            elect_self(&lck);
        }
    }

    struct OnPreVoteRPCDone : public google::protobuf::Closure
    {
        OnPreVoteRPCDone(const PeerId &peer_id_, const int64_t term_,
                         const int64_t ctx_version_, NodeImpl *node_)
            : peer(peer_id_), term(term_), ctx_version(ctx_version_), node(node_)
        {
            node->AddRef();
        }
        virtual ~OnPreVoteRPCDone()
        {
            node->Release();
        }

        void Run()
        {
            do
            {
                if (cntl.ErrorCode() != 0)
                {
                    LOG(WARNING) << "node " << node->node_id()
                                 << " request PreVote from " << peer
                                 << " error: " << cntl.ErrorText();
                    break;
                }
                node->handle_pre_vote_response(peer, term, ctx_version, response, cntl);
            } while (0);
            delete this;
        }

        PeerId peer;
        int64_t term;
        int64_t ctx_version;
        RequestVoteRequest request;
        RequestVoteResponse response;
        brpc::Controller cntl;
        NodeImpl *node;
    };

    // 预选举
    // 在基础的 raft 算法中，当一个 follower 节点与其他节点发生网络分区时，由于心跳超时，会主动发起一次选举，每次选举时会把 term 加一。
    // 由于网络分区的存在，每次 RequestVote RPC 都会超时，结果是，一直不断地发起新的选举，term 会不断增大。
    // 在网络分区恢复，重新加入集群后，其 term 值会被其他节点知晓，导致其他节点更新自己的 term，并变为 follower。
    // 然后触发重新选举，但被隔离的节点日志不是最新，并不会竞选成功，整个集群的状态被该节点扰乱。
    // 在follower节点将要发起一次选举时，会先发起一次 prevote 请求，判断是否能够赢得选举，赢得选举的条件与正常选举相同。
    // 如果可以，则转换为 candidate 状态，增加 term 值，并发起正常的选举。否则不会就不会转换为candidate状态，也不会增加term
    void NodeImpl::pre_vote(std::unique_lock<raft_mutex_t> *lck, bool triggered)
    {
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " term " << _current_term << " start pre_vote";
        // 安装快照时不能发起预投票
        if (_snapshot_executor && _snapshot_executor->is_installing_snapshot())
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " term " << _current_term
                         << " doesn't do pre_vote when installing snapshot as the "
                            " configuration is possibly out of date";
            return;
        }
        if (!_conf.contains(_server_id))
        {
            LOG(WARNING) << "node " << _group_id << ':' << _server_id
                         << " can't do pre_vote as it is not in " << _conf.conf;
            return;
        }

        int64_t old_term = _current_term;
        // get last_log_id outof node mutex
        lck->unlock();
        const LogId last_log_id = _log_manager->last_log_id(true);
        lck->lock();
        // pre_vote need defense ABA after unlock&lock
        if (old_term != _current_term)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " raise term " << _current_term << " when get last_log_id";
            return;
        }

        // 创建ballot并加入票箱，这里可以看出预投票和正式投票的过程是一样的，都是通过Ballot实现的
        _pre_vote_ctx.init(this, triggered);
        std::set<PeerId> peers;
        _conf.list_peers(&peers);

        ////////////
        // 获取当前节点顺序提交的最后日志和乱序提交的最后日志的index
        int64_t last_applied_index = _fsm_caller->last_applied_index();
        int64_t max_applied_index = _fsm_caller->max_applied_index();
        u_int32_t bits[16];
        _fsm_caller->copy_bits(bits, 16);

        ////////////

        // 循环向所有其它的peer节点发送preVote Rpc
        for (std::set<PeerId>::const_iterator
                 iter = peers.begin();
             iter != peers.end(); ++iter)
        {
            // 跳过当前节点自身
            if (*iter == _server_id)
            {
                continue;
            }
            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            brpc::Channel channel;
            if (0 != channel.Init(iter->addr, &options))
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " channel init failed, addr " << iter->addr;
                continue;
            }

            // 收到响应之后的回调, 会执行其Run方法
            OnPreVoteRPCDone *done = new OnPreVoteRPCDone(
                *iter, _current_term, _pre_vote_ctx.version(), this);
            done->cntl.set_timeout_ms(_options.election_timeout_ms);
            done->request.set_group_id(_group_id);
            done->request.set_server_id(_server_id.to_string());
            done->request.set_peer_id(iter->to_string());
            done->request.set_term(_current_term + 1); // next term
            done->request.set_last_log_index(last_log_id.index);
            done->request.set_last_log_term(last_log_id.term);

            ////////////
            ////////////
            done->request.set_last_applied_index(last_applied_index);
            done->request.set_max_applied_index(max_applied_index);
            for (int i = 0; i < 16; i++)
            {
                done->request.add_bits(bits[i]);
            }
            ////////////
            ////////////

            RaftService_Stub stub(&channel);
            // 发送pre_vote rpc请求
            stub.pre_vote(&done->cntl, &done->request, &done->response, done);
        }
        // 投自己一票
        grant_self(&_pre_vote_ctx, lck);
    }

    // in lock
    void NodeImpl::elect_self(std::unique_lock<raft_mutex_t> *lck)
    {
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " term " << _current_term << " start vote and grant vote self";
        if (!_conf.contains(_server_id))
        {
            LOG(WARNING) << "node " << _group_id << ':' << _server_id
                         << " can't do elect_self as it is not in " << _conf.conf;
            return;
        }
        // cancel follower election timer
        // _election_timer 用于开启leader失联后follower的预投票，而当前正在进行正式投票阶段，这表示预投票已经完成了,
        // 因此需要关闭_election_timer避免新一轮的预投票
        if (_state == STATE_FOLLOWER)
        {
            BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                       << " term " << _current_term << " stop election_timer";
            _election_timer.stop();
        }
        // reset leader_id before vote
        // 将当前节点的leader设置为空
        PeerId empty_id;
        butil::Status status;
        status.set_error(ERAFTTIMEDOUT, "A follower's leader_id is reset to NULL "
                                        "as it begins to request_vote.");
        reset_leader_id(empty_id, status);

        // 设置状态为candidate, 将自身的term加1
        _state = STATE_CANDIDATE;
        _current_term++;
        // 表示已经给自己投过票了，这样就不会再给其他节点的投票请求投票了
        _voted_id = _server_id;

        BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                   << " term " << _current_term << " start vote_timer";
        // 启动_vote_timer, 该计时器负责选举阶段的超时
        _vote_timer.start();
        _pre_vote_ctx.reset(this);

        // 为正式投票请求初始化Ballot对象
        _vote_ctx.init(this, false);

        // 获取最新的log
        int64_t old_term = _current_term;
        // get last_log_id outof node mutex
        lck->unlock();
        const LogId last_log_id = _log_manager->last_log_id(true);
        lck->lock();
        // vote need defense ABA after unlock&lock
        if (old_term != _current_term)
        {
            // term changed cause by step_down
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " raise term " << _current_term << " when get last_log_id";
            return;
        }
        _vote_ctx.set_last_log_id(last_log_id);

        // 向所有其他的节点发送RequestVote RPC请求
        std::set<PeerId> peers;
        _conf.list_peers(&peers);
        request_peers_to_vote(peers, NULL);

        //TODO: outof lock
        status = _meta_storage->set_term_and_votedfor(_current_term, _server_id, _v_group_id);
        if (!status.ok())
        {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " fail to set_term_and_votedfor itself when elect_self,"
                          " error: "
                       << status;
            // reset _voted_id to avoid inconsistent cases
            // return immediately without granting _vote_ctx
            _voted_id.reset();
            return;
        }
        // 为自己投票，检查票数，如果票数过半了就调用become_leader()
        grant_self(&_vote_ctx, lck);
    }

    void NodeImpl::request_peers_to_vote(const std::set<PeerId> &peers,
                                         const NodeImpl::DisruptedLeader *disrupted_leader)
    {
        // 循环向所有其他节点发起投票
        for (std::set<PeerId>::const_iterator
                 iter = peers.begin();
             iter != peers.end(); ++iter)
        {
            if (*iter == _server_id)
            {
                continue;
            }
            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            brpc::Channel channel;
            if (0 != channel.Init(iter->addr, &options))
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " channel init failed, addr " << iter->addr;
                continue;
            }

            // 收到响应后的回调, 调用其Run方法
            OnRequestVoteRPCDone *done =
                new OnRequestVoteRPCDone(*iter, _current_term, _vote_ctx.version(), this);
            done->cntl.set_timeout_ms(_options.election_timeout_ms);
            done->request.set_group_id(_group_id);
            done->request.set_server_id(_server_id.to_string());
            done->request.set_peer_id(iter->to_string());
            done->request.set_term(_current_term);
            done->request.set_last_log_index(_vote_ctx.last_log_id().index);
            done->request.set_last_log_term(_vote_ctx.last_log_id().term);

            if (disrupted_leader)
            {
                done->request.mutable_disrupted_leader()
                    ->set_peer_id(disrupted_leader->peer_id.to_string());
                done->request.mutable_disrupted_leader()
                    ->set_term(disrupted_leader->term);
            }

            RaftService_Stub stub(&channel);
            // 发起RPC请求
            stub.request_vote(&done->cntl, &done->request, &done->response, done);
        }
    }

    // in lock
    void NodeImpl::step_down(const int64_t term, bool wakeup_a_candidate,
                             const butil::Status &status)
    {
        BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                   << " term " << _current_term
                   << " stepdown from " << state2str(_state)
                   << " new_term " << term
                   << " wakeup_a_candidate=" << wakeup_a_candidate;

        if (!is_active_state(_state))
        {
            return;
        }
        // delete timer and something else
        if (_state == STATE_CANDIDATE)
        {
            _vote_timer.stop();
            _vote_ctx.reset(this);
        }
        else if (_state == STATE_FOLLOWER)
        {
            _pre_vote_ctx.reset(this);
        }
        else if (_state <= STATE_TRANSFERRING)
        {
            _stepdown_timer.stop();
            _ballot_box->clear_pending_tasks();

            // signal fsm leader stop immediately
            if (_state == STATE_LEADER)
            {
                _leader_lease.on_leader_stop();
                _fsm_caller->on_leader_stop(status);
            }
        }

        // reset leader_id
        PeerId empty_id;
        reset_leader_id(empty_id, status);

        // soft state in memory
        _state = STATE_FOLLOWER;
        // _conf_ctx.reset() will stop replicators of catching up nodes
        _conf_ctx.reset();
        _majority_nodes_readonly = false;

        clear_append_entries_cache();

        if (_snapshot_executor)
        {
            _snapshot_executor->interrupt_downloading_snapshot(term);
        }

        // meta state
        if (term > _current_term)
        {
            _current_term = term;
            _voted_id.reset();
            //TODO: outof lock
            butil::Status status = _meta_storage->set_term_and_votedfor(term, _voted_id, _v_group_id);
            if (!status.ok())
            {
                LOG(ERROR) << "node " << _group_id << ":" << _server_id
                           << " fail to set_term_and_votedfor when step_down, error: "
                           << status;
                // TODO report error
            }
        }

        // stop stagging new node
        if (wakeup_a_candidate)
        {
            _replicator_group.stop_all_and_find_the_next_candidate(
                &_waking_candidate, _conf);
            // FIXME: We issue the RPC in the critical section, which is fine now
            // since the Node is going to quit when reaching the branch
            Replicator::send_timeout_now_and_stop(
                _waking_candidate, _options.election_timeout_ms);
        }
        else
        {
            _replicator_group.stop_all();
        }
        if (_stop_transfer_arg != NULL)
        {
            const int rc = bthread_timer_del(_transfer_timer);
            if (rc == 0)
            {
                // Get the right to delete _stop_transfer_arg.
                delete _stop_transfer_arg;
            } // else on_transfer_timeout will delete _stop_transfer_arg

            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark _stop_transfer_arg to NULL
            _stop_transfer_arg = NULL;
        }
        //  启动 _election_timer
        _election_timer.start();
    }
    // in lock
    void NodeImpl::reset_leader_id(const PeerId &new_leader_id,
                                   const butil::Status &status)
    {
        if (new_leader_id.is_empty())
        {
            if (!_leader_id.is_empty() && _state > STATE_TRANSFERRING)
            {
                LeaderChangeContext stop_following_context(_leader_id,
                                                           _current_term, status);
                _fsm_caller->on_stop_following(stop_following_context);
            }
            _leader_id.reset();
        }
        else
        {
            if (_leader_id.is_empty())
            {
                _pre_vote_ctx.reset(this);
                LeaderChangeContext start_following_context(new_leader_id,
                                                            _current_term, status);
                _fsm_caller->on_start_following(start_following_context);
            }
            _leader_id = new_leader_id;
        }
    }

    // in lock
    void NodeImpl::check_step_down(const int64_t request_term, const PeerId &server_id)
    {
        butil::Status status;
        // 请求中的term比当前节点的term大，当前节点step_down
        if (request_term > _current_term)
        {
            status.set_error(ENEWLEADER, "Raft node receives message from "
                                         "new leader with higher term.");
            step_down(request_term, false, status);
        }
        // 不可能请求的term比当前term小，因为check之前就已经处理了
        // 运行到这里说明请求的term和当前的term相等
        // 如果当前节点不是follower， 进行step_down
        else if (_state != STATE_FOLLOWER)
        {
            status.set_error(ENEWLEADER, "Candidate receives message "
                                         "from new leader with the same term.");
            step_down(request_term, false, status);
        }
        // 运行到此处说明请求的term和当前节点的term相等，且当前节点是follower，但是当前节点还没有 leader
        else if (_leader_id.is_empty())
        {
            status.set_error(ENEWLEADER, "Follower receives message "
                                         "from new leader with the same term.");
            step_down(request_term, false, status);
        }
        // save current leader
        // 如果当前节点还没有leader，则将请求的节点设置为当前节点的leader
        if (_leader_id.is_empty())
        {
            reset_leader_id(server_id, status);
        }
    }

    class LeaderStartClosure : public Closure
    {
    public:
        LeaderStartClosure(StateMachine *fsm, int64_t term) : _fsm(fsm), _term(term) {}
        ~LeaderStartClosure() {}
        void Run()
        {
            if (status().ok())
            {
                _fsm->on_leader_start(_term);
            }
            delete this;
        }

    private:
        StateMachine *_fsm;
        int64_t _term;
    };

    // in lock
    // candidate赢得选举之后会调用become_leader
    void NodeImpl::become_leader()
    {
        CHECK(_state == STATE_CANDIDATE);
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " term " << _current_term
                  << " become leader of group " << _conf.conf
                  << " " << _conf.old_conf;
        // cancel candidate vote timer
        // _vote_timer用于控制请求投票的时间，一旦candidate成为leader就可以停止该计时器
        _vote_timer.stop();
        _vote_ctx.reset(this);

        // 修改当前节点状态为leader并设置当前的leader节点
        _state = STATE_LEADER;
        _leader_id = _server_id;

        // 设置 _replicator_group  的任期
        _replicator_group.reset_term(_current_term);

        // 重置租期
        _follower_lease.reset();
        _leader_lease.on_leader_start(_current_term);

        std::set<PeerId> peers;
        _conf.list_peers(&peers);

        // 为其他节点创建Replicator并保存到ReplicatorrGroup，由ReplicatorGroup进行管理
        for (std::set<PeerId>::const_iterator
                 iter = peers.begin();
             iter != peers.end(); ++iter)
        {
            if (*iter == _server_id)
            {
                continue;
            }

            BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                       << " term " << _current_term
                       << " add replicator " << *iter;
            //TODO: check return code
            _replicator_group.add_replicator(*iter);
        }

        // init commit manager
        // 指定下一个待投票的日志的index
        _ballot_box->reset_pending_index(_log_manager->last_log_index() + 1);

        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.
        CHECK(!_conf_ctx.is_busy());
        // 更新配置
        _conf_ctx.flush(_conf.conf, _conf.old_conf);
        //  启动_stepdown_timer
        _stepdown_timer.start();
    }

    class LeaderStableClosure : public LogManager::StableClosure
    {
    public:
        void Run();

    private:
        LeaderStableClosure(const NodeId &node_id,
                            size_t nentries,
                            BallotBox *ballot_box);
        ~LeaderStableClosure() {}
        friend class NodeImpl;
        NodeId _node_id;
        size_t _nentries;
        BallotBox *_ballot_box;
    };

    LeaderStableClosure::LeaderStableClosure(const NodeId &node_id,
                                             size_t nentries,
                                             BallotBox *ballot_box)
        : _node_id(node_id), _nentries(nentries), _ballot_box(ballot_box)
    {
    }

    void LeaderStableClosure::Run()
    {
        if (status().ok())
        {
            if (_ballot_box)
            {
                // ballot_box check quorum ok, will call fsm_caller

                _ballot_box->commit_at(
                    _first_log_index, _first_log_index + _nentries - 1, _node_id.peer_id);
            }
            int64_t now = butil::cpuwide_time_us();
            if (FLAGS_raft_trace_append_entry_latency &&
                now - metric.start_time_us > (int64_t)FLAGS_raft_append_entry_high_lat_us)
            {
                LOG(WARNING) << "leader append entry latency us " << (now - metric.start_time_us)
                             << " greater than "
                             << FLAGS_raft_append_entry_high_lat_us
                             << metric
                             << " node " << _node_id
                             << " log_index [" << _first_log_index
                             << ", " << _first_log_index + _nentries - 1
                             << "]";
            }
        }
        else
        {
            LOG(ERROR) << "node " << _node_id << " append [" << _first_log_index << ", "
                       << _first_log_index + _nentries - 1 << "] failed";
        }
        delete this;
    }

    // 批量apply参数tasks数组中的task
    void NodeImpl::apply(LogEntryAndClosure tasks[], size_t size)
    {
        g_apply_tasks_batch_counter << size;

        std::vector<LogEntry *> entries;
        entries.reserve(size);
        std::unique_lock<raft_mutex_t> lck(_mutex);
        bool reject_new_user_logs = (_node_readonly || _majority_nodes_readonly);
        // 当前节点不是leader或者集群不可写
        if (_state != STATE_LEADER || reject_new_user_logs)
        {
            butil::Status st;
            if (_state == STATE_LEADER && reject_new_user_logs)
            {
                st.set_error(EREADONLY, "readonly mode reject new user logs");
            }
            else if (_state != STATE_TRANSFERRING)
            {
                st.set_error(EPERM, "is not leader");
            }
            else
            {
                st.set_error(EBUSY, "is transferring leadership");
            }
            lck.unlock();
            BRAFT_VLOG << "node " << _group_id << ":" << _server_id << " can't apply : " << st;
            // 执行task的回调函数，释放task
            for (size_t i = 0; i < size; ++i)
            {
                tasks[i].entry->Release();
                if (tasks[i].done)
                {
                    tasks[i].done->status() = st;
                    run_closure_in_bthread(tasks[i].done);
                }
            }
            return;
        }
        // 组装log entry
        for (size_t i = 0; i < size; ++i)
        {
            // expected_term是啥？？？
            if (tasks[i].expected_term != -1 && tasks[i].expected_term != _current_term)
            {
                BRAFT_VLOG << "node " << _group_id << ":" << _server_id
                           << " can't apply taks whose expected_term=" << tasks[i].expected_term
                           << " doesn't match current_term=" << _current_term;
                if (tasks[i].done)
                {
                    tasks[i].done->status().set_error(
                        EPERM, "expected_term=%" PRId64 " doesn't match current_term=%" PRId64,
                        tasks[i].expected_term, _current_term);
                    run_closure_in_bthread(tasks[i].done);
                }
                tasks[i].entry->Release();
                continue;
            }
            entries.push_back(tasks[i].entry);
            entries.back()->id.term = _current_term;
            entries.back()->type = ENTRY_TYPE_DATA;
            // 如果刚把配置更新entry放到投票箱, 这里 _conf.stable() 会返回false, 第二个参数会是 _conf.old_conf
            // 所以在此时产生的任务需要新旧两个配置共同决定是否commit，也就是JOINT状态
            _ballot_box->append_pending_task(_conf.conf,
                                             _conf.stable() ? NULL : &_conf.old_conf,
                                             tasks[i].done);
        }

        // 通过log_manager执行append日志操作
        _log_manager->append_entries(&entries,
                                     new LeaderStableClosure(
                                         NodeId(_group_id, _server_id),
                                         entries.size(),
                                         _ballot_box));
        // update _conf.first
        _log_manager->check_and_set_configuration(&_conf);
    }

    void NodeImpl::unsafe_apply_configuration(const Configuration &new_conf,
                                              const Configuration *old_conf,
                                              bool leader_start)
    {
        CHECK(_conf_ctx.is_busy());
        // 生成 ENTRY_TYPE_CONFIGURATION 类型的Entry,表示这是一个用于配置更新的entry
        LogEntry *entry = new LogEntry();
        entry->AddRef();
        entry->id.term = _current_term;
        entry->type = ENTRY_TYPE_CONFIGURATION;
        entry->peers = new std::vector<PeerId>;
        // 将entry的peers设置为新配置下的全部节点
        new_conf.list_peers(entry->peers);
        // 将entry的old_peers设置为原有配置下的全部节点
        if (old_conf)
        {
            entry->old_peers = new std::vector<PeerId>;
            old_conf->list_peers(entry->old_peers);
        }

        //
        ConfigurationChangeDone *configuration_change_done =
            new ConfigurationChangeDone(this, _current_term, leader_start, _leader_lease.lease_epoch());
        // Use the new_conf to deal the quorum of this very log

        // 将任务放到投票箱获取投票, 成功提交后执行 ConfigurationChangeDone::Run, 进入下一个阶段
        // 这里有两种配置，因此有两种法定票数，只要有一种能够减到0就表示这个配置entry可以commit了。
        _ballot_box->append_pending_task(new_conf, old_conf, configuration_change_done);

        std::vector<LogEntry *> entries;
        entries.push_back(entry);
        // 调用LogManager::append_entries把entry添加到到内存并持久化
        _log_manager->append_entries(&entries,
                                     new LeaderStableClosure(
                                         NodeId(_group_id, _server_id),
                                         1u, _ballot_box));
        // 这里就是检查_conf是不是最新配置，如果不是就设置为最新配置
        _log_manager->check_and_set_configuration(&_conf);
    }

    // 处理预投票请求
    // 给对方投票的标准:
    // 1. 当前节点的term值大于等于请求携带的term
    // 2. 当前节点的log比请求节点的log新(先比较term，term大的更新，term相同时比较index，index大的更新)
    // 3. 以上两个条件可以保证投票，但不保证能够使请求节点的Ballot减1，因为还需要判断租约是否过期。如果没到期即使满足投票条件，请求节点也不能将法定票数减1
    int NodeImpl::handle_pre_vote_request(const RequestVoteRequest *request,
                                          RequestVoteResponse *response, brpc::Controller *cntl)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // 非活跃状态的节点不能进行预投票
        // 只有处于leader、transferring、follower和candidate状态的节点才能响应预投票
        if (!is_active_state(_state))
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " is not in active state "
                         << "current_term "
                         << saved_current_term
                         << " state " << state2str(saved_state);
            return EINVAL;
        }

        PeerId candidate_id;
        if (0 != candidate_id.parse(request->server_id()))
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received PreVote from " << request->server_id()
                         << " server_id bad format";
            return EINVAL;
        }

        bool granted = false;
        bool rejected_by_lease = false;
        do
        {
            // 请求的节点的term比当前节点的term小不会给他投票
            if (request->term() < _current_term)
            {
                // ignore older term
                LOG(INFO) << "node " << _group_id << ":" << _server_id
                          << " ignore PreVote from " << request->server_id()
                          << " in term " << request->term()
                          << " current_term " << _current_term;
                break;
            }

            // get last_log_id outof node mutex
            lck.unlock();
            LogId last_log_id = _log_manager->last_log_id(true);
            lck.lock();
            // pre_vote not need ABA check after unlock&lock

            // 运行到这里说明至少请求的节点的term大于等于当前节点的term

            // votable_time其实就是租约到期的剩余时间
            int64_t votable_time = _follower_lease.votable_time_from_now();
            // 只有请求的节点的日志比当前节点的日志更新才能够投票给它，判断更新的原则是:
            // term相等则比较index,index更大日志更新；否则term大的日志更新
            // !!!也就是说比较那个日志更新是先比较term再比较index的!!!
            bool grantable = (LogId(request->last_log_index(), request->last_log_term()) >= last_log_id);
            // 到这里说明是满足执行条件的，因为对方的日志比自己新；但是如果租约还没到期也要拒绝
            if (grantable)
            {
                granted = (votable_time == 0);
                rejected_by_lease = (votable_time > 0);
            }

            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " received PreVote from " << request->server_id()
                      << " in term " << request->term()
                      << " current_term " << _current_term
                      << " granted " << granted
                      << " rejected_by_lease " << rejected_by_lease;

        } while (0);

        response->set_term(_current_term);
        response->set_granted(granted);
        response->set_rejected_by_lease(rejected_by_lease);
        response->set_disrupted(_state == STATE_LEADER);
        response->set_previous_term(_current_term);

        //////////////
        //////////////
        //  不管给不给他投票都把对方缺失的日志发过去

        int64_t last_applied_index = request->last_applied_index();
        int bits_size = request->bits_size();
        int64_t last_log_index = _log_manager->last_log_index();
        for (int64_t i = last_applied_index + 1; i <= last_log_index; i++)
        {
            u_int32_t num = (u_int32_t)request->bits((i - last_applied_index) / 32);
            u_int32_t bit = num >> (31 - (i - last_applied_index) % 32);
            // 等于1说明对方存在
            if ((bit & 1) == 1)
            {
                continue;
            }
            // 对方没有就发过去
            else
            {
                EntryMeta em;
                // 从log_manager中获取
                LogEntry *entry = _log_manager->get_entry(i);
                // 自己也没有
                if (entry == NULL)
                {
                    continue;
                }
                em.set_term(entry->id.term);
                em.set_type(entry->type);
                if (entry->peers != NULL)
                {
                    for (size_t i = 0; i < entry->peers->size(); ++i)
                    {
                        em.add_peers((*entry->peers)[i].to_string());
                    }
                    if (entry->old_peers != NULL)
                    {
                        for (size_t i = 0; i < entry->old_peers->size(); ++i)
                        {
                            em.add_old_peers((*entry->old_peers)[i].to_string());
                        }
                    }
                }
                em.set_data_len(entry->data.length());
                em.set_index(i);
                cntl->response_attachment().append(entry->data);
                entry->Release();
                response->add_entries()->Swap(&em);
            }
        }

        //////////////
        //////////////

        return 0;
    }

    // 收到其他节点的request_vote请求后的处理逻辑
    int NodeImpl::handle_request_vote_request(const RequestVoteRequest *request,
                                              RequestVoteResponse *response)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        if (!is_active_state(_state))
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " is not in active state "
                         << "current_term "
                         << saved_current_term
                         << " state " << state2str(saved_state);
            return EINVAL;
        }

        // 从请求中获取请求投票的节点ID
        PeerId candidate_id;
        if (0 != candidate_id.parse(request->server_id()))
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received RequestVote from " << request->server_id()
                         << " server_id bad format";
            return EINVAL;
        }

        PeerId disrupted_leader_id;
        if (_state == STATE_FOLLOWER &&
            request->has_disrupted_leader() &&
            _current_term == request->disrupted_leader().term() &&
            0 == disrupted_leader_id.parse(request->disrupted_leader().peer_id()) &&
            _leader_id == disrupted_leader_id)
        {
            // The candidate has already disrupted the old leader, we
            // can expire the lease safely.
            _follower_lease.expire();
        }

        bool disrupted = false;
        int64_t previous_term = _current_term;
        bool rejected_by_lease = false;
        do
        {
            // ignore older term
            // 如果请求的term比自身term小, 则忽略该请求,即不投票
            if (request->term() < _current_term)
            {
                // ignore older term
                LOG(INFO) << "node " << _group_id << ":" << _server_id
                          << " ignore RequestVote from " << request->server_id()
                          << " in term " << request->term()
                          << " current_term " << _current_term;
                break;
            }

            // 运行到此处说明请求的term大于等于当前节点的term
            // get last_log_id outof node mutex
            lck.unlock();
            // 获取当前节点的最后一条log的ID
            LogId last_log_id = _log_manager->last_log_id(true);
            lck.lock();

            // vote need ABA check after unlock&lock
            if (previous_term != _current_term)
            {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " raise term " << _current_term << " when get last_log_id";
                break;
            }

            // 请求方的日志是否比当前节点的日志更新
            bool log_is_ok = (LogId(request->last_log_index(), request->last_log_term()) >= last_log_id);
            // 租期到期剩余时间
            int64_t votable_time = _follower_lease.votable_time_from_now();

            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " received RequestVote from " << request->server_id()
                      << " in term " << request->term()
                      << " current_term " << _current_term
                      << " log_is_ok " << log_is_ok
                      << " votable_time " << votable_time;

            // if the vote is rejected by lease, tell the candidate
            // 租期没过不予投票
            if (votable_time > 0)
            {
                rejected_by_lease = log_is_ok;
                break;
            }

            // increase current term, change state to follower
            // 如果对方的term比自己大，更新自己的term并回退到follower状态
            if (request->term() > _current_term)
            {
                butil::Status status;
                status.set_error(EHIGHERTERMREQUEST, "Raft node receives higher term "
                                                     "request_vote_request.");
                disrupted = (_state <= STATE_TRANSFERRING);
                step_down(request->term(), false, status);
            }

            // save
            // request中的log比自身新且当前节点还没给别的节点投票
            if (log_is_ok && _voted_id.is_empty())
            {
                butil::Status status;
                status.set_error(EVOTEFORCANDIDATE, "Raft node votes for some candidate, "
                                                    "step down to restart election_timer.");
                // 当前节点回退到follower状态
                step_down(request->term(), false, status);
                // 表示已经给该节点投票了，就不会再投票给别的节点了
                _voted_id = candidate_id;
                //TODO: outof lock
                status = _meta_storage->set_term_and_votedfor(_current_term, candidate_id, _v_group_id);
                if (!status.ok())
                {
                    LOG(ERROR) << "node " << _group_id << ":" << _server_id
                               << " refuse to vote for " << request->server_id()
                               << " because failed to set_votedfor it, error: "
                               << status;
                    // reset _voted_id to response set_granted(false)
                    _voted_id.reset();
                }
            }
        } while (0);

        // 如果当前节点是leader或者正在进行TRANSFERRING(leader权禅让)，set_disrupted就为true
        response->set_disrupted(disrupted);
        response->set_previous_term(previous_term);
        // 返回当前节点的term
        response->set_term(_current_term);
        response->set_granted(request->term() == _current_term && _voted_id == candidate_id);
        // 如果对方满足投票要求，但是因为当前节点租期还没过导致无法给对方投票，set_rejected_by_lease就为true
        response->set_rejected_by_lease(rejected_by_lease);
        return 0;
    }

    class FollowerStableClosure : public LogManager::StableClosure
    {
    public:
        FollowerStableClosure(
            brpc::Controller *cntl,
            const AppendEntriesRequest *request,
            AppendEntriesResponse *response,
            google::protobuf::Closure *done,
            NodeImpl *node,
            int64_t term)
            : _cntl(cntl), _request(request), _response(response), _done(done), _node(node), _term(term)
        {
            _node->AddRef();
        }
        void Run()
        {
            run();
            delete this;
        }

    private:
        ~FollowerStableClosure()
        {
            if (_node)
            {
                _node->Release();
            }
        }
        // follower收到leader的entry，等到entry持久化之后回调执行
        void run()
        {
            brpc::ClosureGuard done_guard(_done);
            if (!status().ok())
            {
                _cntl->SetFailed(status().error_code(), "%s",
                                 status().error_cstr());
                return;
            }
            std::unique_lock<raft_mutex_t> lck(_node->_mutex);
            // _term 是收到leader的entry时当前节点的term，_node->_current_term是进行回调时当前节点的term
            // 不相等说明在此期间出现了新的leader，当前节点的term被新的leader改变了。
            // 这个时候不能向leader投票，即使投票了这些entry也会被截断
            if (_term != _node->_current_term)
            {
                // The change of term indicates that leader has been changed during
                // appending entries, so we can't respond ok to the old leader
                // because we are not sure if the appended logs would be truncated
                // by the new leader:
                //  - If they won't be truncated and we respond failure to the old
                //    leader, the new leader would know that they are stored in this
                //    peer and they will be eventually committed when the new leader
                //    found that quorum of the cluster have stored.
                //  - If they will be truncated and we responded success to the old
                //    leader, the old leader would possibly regard those entries as
                //    committed (very likely in a 3-nodes cluster) and respond
                //    success to the clients, which would break the rule that
                //    committed entries would never be truncated.
                // So we have to respond failure to the old leader and set the new
                // term to make it stepped down if it didn't.
                _response->set_success(false);
                _response->set_term(_node->_current_term);
                return;
            }
            // It's safe to release lck as we know everything is ok at this point.
            lck.unlock();

            // DON'T touch _node any more
            _response->set_success(true);
            _response->set_term(_term);

            // 获取已经commit的日志的index
            // _request->committed_index()是leader发出的append entry请求的committed_index
            const int64_t committed_index =
                std::min(_request->committed_index(),
                         // ^^^ committed_index is likely less than the
                         // last_log_index
                         _request->prev_log_index() + _request->entries_size()
                         // ^^^ The logs after the appended entries are
                         // untrustable so we can't commit them even if their
                         // indexes are less than request->committed_index()
                );
            //_ballot_box is thread safe and tolerates disorder.
            // 更新commited_index
            // 调用状态机操作
            _node->_ballot_box->set_last_committed_index(committed_index);
            int64_t now = butil::cpuwide_time_us();
            if (FLAGS_raft_trace_append_entry_latency && now - metric.start_time_us >
                                                             (int64_t)FLAGS_raft_append_entry_high_lat_us)
            {
                LOG(WARNING) << "follower append entry latency us " << (now - metric.start_time_us)
                             << " greater than "
                             << FLAGS_raft_append_entry_high_lat_us
                             << metric
                             << " node " << _node->node_id()
                             << " log_index [" << _request->prev_log_index() + 1
                             << ", " << _request->prev_log_index() + _request->entries_size() - 1
                             << "]";
            }
        }

        brpc::Controller *_cntl;
        const AppendEntriesRequest *_request;
        AppendEntriesResponse *_response;
        google::protobuf::Closure *_done;
        NodeImpl *_node;
        int64_t _term;
    };

    // 节点接收到 _append_entries_ 请求后的处理
    // from_append_entries_cache默认是false
    void NodeImpl::handle_append_entries_request(brpc::Controller *cntl,
                                                 const AppendEntriesRequest *request,
                                                 AppendEntriesResponse *response,
                                                 google::protobuf::Closure *done,
                                                 bool from_append_entries_cache)
    {
        std::vector<LogEntry *> entries;
        entries.reserve(request->entries_size());
        brpc::ClosureGuard done_guard(done);
        std::unique_lock<raft_mutex_t> lck(_mutex);

        // pre set term, to avoid get term in lock
        // 将当前的term发送给leader
        response->set_term(_current_term);

        // 只有活跃状态的节点执行append_entries
        if (!is_active_state(_state))
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " is not in active state "
                         << "current_term " << saved_current_term
                         << " state " << state2str(saved_state);
            cntl->SetFailed(EINVAL, "node %s:%s is not in active state, state %s",
                            _group_id.c_str(), _server_id.to_string().c_str(), state2str(saved_state));
            return;
        }

        PeerId server_id;

        //从请求中获取请求的server_id
        if (0 != server_id.parse(request->server_id()))
        {
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received AppendEntries from " << request->server_id()
                         << " server_id bad format";
            cntl->SetFailed(brpc::EREQUEST,
                            "Fail to parse server_id `%s'",
                            request->server_id().c_str());
            return;
        }

        // check stale term
        // 发起请求节点的term比当前节点的term小则拒绝该请求
        if (request->term() < _current_term)
        {
            const int64_t saved_current_term = _current_term;
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " ignore stale AppendEntries from " << request->server_id()
                         << " in term " << request->term()
                         << " current_term " << saved_current_term;
            response->set_success(false);
            response->set_term(saved_current_term);
            return;
        }

        // check term and state to step down
        check_step_down(request->term(), server_id);

        // 当前节点已经有另一个leader，拒绝该请求
        if (server_id != _leader_id)
        {
            LOG(ERROR) << "Another peer " << _group_id << ":" << server_id
                       << " declares that it is the leader at term=" << _current_term
                       << " which was occupied by leader=" << _leader_id;
            // Increase the term by 1 and make both leaders step down to minimize the
            // loss of split brain
            butil::Status status;
            status.set_error(ELEADERCONFLICT, "More than one leader in the same term.");
            step_down(request->term() + 1, false, status);
            response->set_success(false);
            response->set_term(request->term() + 1);
            return;
        }

        if (!from_append_entries_cache)
        {
            // Requests from cache already updated timestamp
            _follower_lease.renew(_leader_id);
        }

        // 当前节点正在安装快照则拒绝该请求
        if (request->entries_size() > 0 &&
            (_snapshot_executor && _snapshot_executor->is_installing_snapshot()))
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " received append entries while installing snapshot";
            cntl->SetFailed(EBUSY, "Is installing snapshot");
            return;
        }

        // leader节点前一个log entry的term和index
        const int64_t prev_log_index = request->prev_log_index();
        const int64_t prev_log_term = request->prev_log_term();
        // 当前节点在该位置处log entry的term
        const int64_t local_prev_log_term = _log_manager->get_term(prev_log_index);

        // 当前节点在该位置处没有对应的term则拒绝append
        if (local_prev_log_term != prev_log_term)
        {
            int64_t last_index = _log_manager->last_log_index();
            int64_t saved_term = request->term();
            int saved_entries_size = request->entries_size();
            std::string rpc_server_id = request->server_id();
            if (!from_append_entries_cache &&
                handle_out_of_order_append_entries(
                    cntl, request, response, done, last_index))
            {
                // It's not safe to touch cntl/request/response/done after this point,
                // since the ownership is tranfered to the cache.
                lck.unlock();
                done_guard.release();
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                             << " cache out-of-order AppendEntries from "
                             << rpc_server_id
                             << " in term " << saved_term
                             << " prev_log_index " << prev_log_index
                             << " prev_log_term " << prev_log_term
                             << " local_prev_log_term " << local_prev_log_term
                             << " last_log_index " << last_index
                             << " entries_size " << saved_entries_size;
                return;
            }

            response->set_success(false);
            // 返回当前节点的任期和最新的log entry的index以便leader确定重发的位置
            response->set_term(_current_term);
            response->set_last_log_index(last_index);
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " reject term_unmatched AppendEntries from "
                         << request->server_id()
                         << " in term " << request->term()
                         << " prev_log_index " << request->prev_log_index()
                         << " prev_log_term " << request->prev_log_term()
                         << " local_prev_log_term " << local_prev_log_term
                         << " last_log_index " << last_index
                         << " entries_size " << request->entries_size()
                         << " from_append_entries_cache: " << from_append_entries_cache;
            return;
        }

        // 空的entry, 也就是不带数据的entry
        if (request->entries_size() == 0)
        {
            response->set_success(true);
            response->set_term(_current_term);
            response->set_last_log_index(_log_manager->last_log_index());
            response->set_readonly(_node_readonly);
            lck.unlock();
            // see the comments at FollowerStableClosure::run()
            _ballot_box->set_last_committed_index(
                std::min(request->committed_index(),
                         prev_log_index));
            return;
        }

        // Parse request
        // 到这里说明entry里面是有数据的
        butil::IOBuf data_buf;
        data_buf.swap(cntl->request_attachment());
        int64_t index = prev_log_index;
        for (int i = 0; i < request->entries_size(); i++)
        {
            index++;
            const EntryMeta &entry = request->entries(i);
            if (entry.type() != ENTRY_TYPE_UNKNOWN)
            {
                LogEntry *log_entry = new LogEntry();
                log_entry->AddRef();
                log_entry->id.term = entry.term();
                log_entry->id.index = index;
                log_entry->type = (EntryType)entry.type();
                if (entry.peers_size() > 0)
                {
                    log_entry->peers = new std::vector<PeerId>;
                    for (int i = 0; i < entry.peers_size(); i++)
                    {
                        log_entry->peers->push_back(entry.peers(i));
                    }
                    CHECK_EQ(log_entry->type, ENTRY_TYPE_CONFIGURATION);
                    if (entry.old_peers_size() > 0)
                    {
                        log_entry->old_peers = new std::vector<PeerId>;
                        for (int i = 0; i < entry.old_peers_size(); i++)
                        {
                            log_entry->old_peers->push_back(entry.old_peers(i));
                        }
                    }
                }
                else
                {
                    CHECK_NE(entry.type(), ENTRY_TYPE_CONFIGURATION);
                }
                if (entry.has_data_len())
                {
                    int len = entry.data_len();
                    data_buf.cutn(&log_entry->data, len);
                }
                entries.push_back(log_entry);
            }
        }

        // check out-of-order cache
        check_append_entries_cache(index);

        // follower的回调函数
        FollowerStableClosure *c = new FollowerStableClosure(
            cntl, request, response, done_guard.release(),
            this, _current_term);
        // #####
        _log_manager->append_entries(&entries, c);

        // update configuration after _log_manager updated its memory status
        _log_manager->check_and_set_configuration(&_conf);
    }

    int NodeImpl::increase_term_to(int64_t new_term, const butil::Status &status)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (new_term <= _current_term)
        {
            return EINVAL;
        }
        step_down(new_term, false, status);
        return 0;
    }

    void NodeImpl::after_shutdown(NodeImpl *node)
    {
        return node->after_shutdown();
    }

    void NodeImpl::after_shutdown()
    {
        std::vector<Closure *> saved_done;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK_EQ(STATE_SHUTTING, _state);
            _state = STATE_SHUTDOWN;
            std::swap(saved_done, _shutdown_continuations);
        }
        Release();
        for (size_t i = 0; i < saved_done.size(); ++i)
        {
            if (NULL == saved_done[i])
            {
                continue;
            }
            run_closure_in_bthread(saved_done[i]);
        }
    }

    // follower收到InstallSnapshot RPC后，会调用此函数
    void NodeImpl::handle_install_snapshot_request(brpc::Controller *cntl,
                                                   const InstallSnapshotRequest *request,
                                                   InstallSnapshotResponse *response,
                                                   google::protobuf::Closure *done)
    {
        brpc::ClosureGuard done_guard(done);

        // snapshot需要在新的线程中进行
        if (_snapshot_executor == NULL)
        {
            cntl->SetFailed(EINVAL, "Not support snapshot");
            return;
        }
        PeerId server_id;
        if (0 != server_id.parse(request->server_id()))
        {
            cntl->SetFailed(brpc::EREQUEST, "Fail to parse server_id=`%s'",
                            request->server_id().c_str());
            return;
        }
        std::unique_lock<raft_mutex_t> lck(_mutex);

        if (!is_active_state(_state))
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            lck.unlock();
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " is not in active state "
                         << "current_term "
                         << saved_current_term << " state " << state2str(saved_state);
            cntl->SetFailed(EINVAL, "node %s:%s is not in active state, state %s",
                            _group_id.c_str(), _server_id.to_string().c_str(), state2str(saved_state));
            return;
        }

        // check stale term
        // leader的term比当前节点小, 拒绝该请求
        if (request->term() < _current_term)
        {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                         << " ignore stale InstallSnapshot from " << request->server_id()
                         << " in term " << request->term()
                         << " current_term " << _current_term;
            response->set_term(_current_term);
            response->set_success(false);
            return;
        }

        check_step_down(request->term(), server_id);

        // 当前节点已经有另一个leader，说明发生了脑裂，通过返回比请求中term更大的term使发送请求的leader自动执行step_down
        if (server_id != _leader_id)
        {
            LOG(ERROR) << "Another peer " << _group_id << ":" << server_id
                       << " declares that it is the leader at term=" << _current_term
                       << " which was occupied by leader=" << _leader_id;
            // Increase the term by 1 and make both leaders step down to minimize the
            // loss of split brain
            butil::Status status;
            status.set_error(ELEADERCONFLICT, "More than one leader in the same term.");
            step_down(request->term() + 1, false, status);
            response->set_success(false);
            response->set_term(request->term() + 1);
            return;
        }
        clear_append_entries_cache();
        lck.unlock();
        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received InstallSnapshotRequest"
                  << " last_included_log_index="
                  << request->meta().last_included_index()
                  << " last_include_log_term="
                  << request->meta().last_included_term()
                  << " from " << server_id
                  << " when last_log_id=" << _log_manager->last_log_id();
        // 调用SnapshotExecutor::install_snapshot
        return _snapshot_executor->install_snapshot(
            cntl, request, response, done_guard.release());
    }

    void NodeImpl::update_configuration_after_installing_snapshot()
    {
        BAIDU_SCOPED_LOCK(_mutex);
        _log_manager->check_and_set_configuration(&_conf);
    }

    butil::Status NodeImpl::read_committed_user_log(const int64_t index, UserLog *user_log)
    {
        if (index <= 0)
        {
            return butil::Status(EINVAL, "request index:%" PRId64 " is invalid.", index);
        }
        const int64_t saved_last_applied_index = _fsm_caller->last_applied_index();
        if (index > saved_last_applied_index)
        {
            return butil::Status(ENOMOREUSERLOG, "request index:%" PRId64 " is greater"
                                                 " than last_applied_index:%" PRId64,
                                 index, saved_last_applied_index);
        }
        int64_t cur_index = index;
        LogEntry *entry = _log_manager->get_entry(cur_index);
        if (entry == NULL)
        {
            return butil::Status(ELOGDELETED, "user log is deleted at index:%" PRId64, index);
        }
        do
        {
            if (entry->type == ENTRY_TYPE_DATA)
            {
                user_log->set_log_index(cur_index);
                user_log->set_log_data(entry->data);
                entry->Release();
                return butil::Status();
            }
            else
            {
                entry->Release();
                ++cur_index;
            }
            if (cur_index > saved_last_applied_index)
            {
                return butil::Status(ENOMOREUSERLOG, "no user log between index:%" PRId64 " and last_applied_index:%" PRId64, index, saved_last_applied_index);
            }
            entry = _log_manager->get_entry(cur_index);
        } while (entry != NULL);
        // entry is likely to be NULL because snapshot is done after
        // getting saved_last_applied_index.
        return butil::Status(ELOGDELETED, "user log is deleted at index:%" PRId64, cur_index);
    }

    void NodeImpl::describe(std::ostream &os, bool use_html)
    {
        PeerId leader;
        std::vector<ReplicatorId> replicators;
        std::unique_lock<raft_mutex_t> lck(_mutex);
        const State st = _state;
        if (st == STATE_FOLLOWER)
        {
            leader = _leader_id;
        }
        const int64_t term = _current_term;
        const int64_t conf_index = _conf.id.index;
        //const int ref_count = ref_count_;
        std::vector<PeerId> peers;
        _conf.conf.list_peers(&peers);

        const std::string is_changing_conf = _conf_ctx.is_busy() ? "YES" : "NO";
        const char *conf_statge = _conf_ctx.stage_str();
        // new_peers and old_peers during all conf-change stages, namely
        // STAGE_CATCHING_UP->STAGE_JOINT->STAGE_STABLE
        std::vector<PeerId> new_peers;
        _conf_ctx.list_new_peers(&new_peers);
        std::vector<PeerId> old_peers;
        _conf_ctx.list_old_peers(&old_peers);

        // No replicator attached to nodes that are not leader;
        _replicator_group.list_replicators(&replicators);
        const int64_t leader_timestamp = _follower_lease.last_leader_timestamp();
        const bool readonly = (_node_readonly || _majority_nodes_readonly);
        lck.unlock();
        const char *newline = use_html ? "<br>" : "\r\n";
        os << "peer_id: " << _server_id << newline;
        os << "state: " << state2str(st) << newline;
        os << "readonly: " << readonly << newline;
        os << "term: " << term << newline;
        os << "conf_index: " << conf_index << newline;
        os << "peers:";
        for (size_t j = 0; j < peers.size(); ++j)
        {
            os << ' ';
            if (use_html && peers[j] != _server_id)
            {
                os << "<a href=\"http://" << peers[j].addr
                   << "/raft_stat/" << _group_id << "\">";
            }
            os << peers[j];
            if (use_html && peers[j] != _server_id)
            {
                os << "</a>";
            }
        }
        os << newline; // newline for peers

        // info of configuration change
        if (st == STATE_LEADER)
        {
            os << "changing_conf: " << is_changing_conf
               << "    stage: " << conf_statge << newline;
        }
        if (!new_peers.empty())
        {
            os << "new_peers:";
            for (size_t j = 0; j < new_peers.size(); ++j)
            {
                os << ' ';
                if (use_html && new_peers[j] != _server_id)
                {
                    os << "<a href=\"http://" << new_peers[j].addr
                       << "/raft_stat/" << _group_id << "\">";
                }
                os << new_peers[j];
                if (use_html && new_peers[j] != _server_id)
                {
                    os << "</a>";
                }
            }
            os << newline; // newline for new_peers
        }
        if (!old_peers.empty())
        {
            os << "old_peers:";
            for (size_t j = 0; j < old_peers.size(); ++j)
            {
                os << ' ';
                if (use_html && old_peers[j] != _server_id)
                {
                    os << "<a href=\"http://" << old_peers[j].addr
                       << "/raft_stat/" << _group_id << "\">";
                }
                os << old_peers[j];
                if (use_html && old_peers[j] != _server_id)
                {
                    os << "</a>";
                }
            }
            os << newline; // newline for old_peers
        }

        if (st == STATE_FOLLOWER)
        {
            os << "leader: ";
            if (use_html)
            {
                os << "<a href=\"http://" << leader.addr
                   << "/raft_stat/" << _group_id << "\">"
                   << leader << "</a>";
            }
            else
            {
                os << leader;
            }
            os << newline;
            os << "last_msg_to_now: " << butil::monotonic_time_ms() - leader_timestamp
               << newline;
        }

        // Show timers
        os << "election_timer: ";
        _election_timer.describe(os, use_html);
        os << newline;
        os << "vote_timer: ";
        _vote_timer.describe(os, use_html);
        os << newline;
        os << "stepdown_timer: ";
        _stepdown_timer.describe(os, use_html);
        os << newline;
        os << "snapshot_timer: ";
        _snapshot_timer.describe(os, use_html);
        os << newline;

        _log_manager->describe(os, use_html);
        _fsm_caller->describe(os, use_html);
        _ballot_box->describe(os, use_html);
        if (_snapshot_executor)
        {
            _snapshot_executor->describe(os, use_html);
        }
        for (size_t i = 0; i < replicators.size(); ++i)
        {
            Replicator::describe(replicators[i], os, use_html);
        }
    }

    void NodeImpl::get_status(NodeStatus *status)
    {
        if (status == NULL)
        {
            return;
        }

        std::vector<PeerId> peers;
        std::vector<std::pair<PeerId, ReplicatorId>> replicators;
        std::unique_lock<raft_mutex_t> lck(_mutex);
        status->state = _state;
        status->term = _current_term;
        status->peer_id = _server_id;
        status->readonly = (_node_readonly || _majority_nodes_readonly);
        _conf.conf.list_peers(&peers);
        _replicator_group.list_replicators(&replicators);
        lck.unlock();

        if (status->state == STATE_LEADER ||
            status->state == STATE_TRANSFERRING)
        {
            status->leader_id = _server_id;
        }
        else if (status->state == STATE_FOLLOWER)
        {
            status->leader_id = _leader_id;
        }

        LogManagerStatus log_manager_status;
        _log_manager->get_status(&log_manager_status);
        status->known_applied_index = log_manager_status.known_applied_index;
        status->first_index = log_manager_status.first_index;
        status->last_index = log_manager_status.last_index;
        status->disk_index = log_manager_status.disk_index;

        BallotBoxStatus ballot_box_status;
        _ballot_box->get_status(&ballot_box_status);
        status->committed_index = ballot_box_status.committed_index;
        status->pending_index = ballot_box_status.pending_index;
        status->pending_queue_size = ballot_box_status.pending_queue_size;

        status->applying_index = _fsm_caller->applying_index();

        if (replicators.size() == 0)
        {
            return;
        }

        for (size_t i = 0; i < peers.size(); ++i)
        {
            if (peers[i] == _server_id)
            {
                continue;
            }
            status->stable_followers.insert(std::make_pair(peers[i], PeerStatus()));
        }

        for (size_t i = 0; i < replicators.size(); ++i)
        {
            NodeStatus::PeerStatusMap::iterator it =
                status->stable_followers.find(replicators[i].first);
            if (it == status->stable_followers.end())
            {
                it = status->unstable_followers.insert(
                                                   std::make_pair(replicators[i].first, PeerStatus()))
                         .first;
            }
            Replicator::get_status(replicators[i].second, &(it->second));
        }
    }

    void NodeImpl::stop_replicator(const std::set<PeerId> &keep,
                                   const std::set<PeerId> &drop)
    {
        for (std::set<PeerId>::const_iterator
                 iter = drop.begin();
             iter != drop.end(); ++iter)
        {
            if (keep.find(*iter) == keep.end() && *iter != _server_id)
            {
                _replicator_group.stop_replicator(*iter);
            }
        }
    }

    bool NodeImpl::handle_out_of_order_append_entries(brpc::Controller *cntl,
                                                      const AppendEntriesRequest *request,
                                                      AppendEntriesResponse *response,
                                                      google::protobuf::Closure *done,
                                                      int64_t local_last_index)
    {
        if (!FLAGS_raft_enable_append_entries_cache ||
            local_last_index >= request->prev_log_index() ||
            request->entries_size() == 0)
        {
            return false;
        }
        if (!_append_entries_cache)
        {
            _append_entries_cache = new AppendEntriesCache(this, ++_append_entries_cache_version);
        }
        AppendEntriesRpc *rpc = new AppendEntriesRpc;
        rpc->cntl = cntl;
        rpc->request = request;
        rpc->response = response;
        rpc->done = done;
        rpc->receive_time_ms = butil::gettimeofday_ms();
        // 将收到的RPC请求缓存起来
        bool rc = _append_entries_cache->store(rpc);
        if (!rc && _append_entries_cache->empty())
        {
            delete _append_entries_cache;
            _append_entries_cache = NULL;
        }
        return rc;
    }

    void NodeImpl::check_append_entries_cache(int64_t local_last_index)
    {
        if (!_append_entries_cache)
        {
            return;
        }
        _append_entries_cache->process_runable_rpcs(local_last_index);
        if (_append_entries_cache->empty())
        {
            delete _append_entries_cache;
            _append_entries_cache = NULL;
        }
    }

    void NodeImpl::clear_append_entries_cache()
    {
        if (!_append_entries_cache)
        {
            return;
        }
        _append_entries_cache->clear();
        delete _append_entries_cache;
        _append_entries_cache = NULL;
    }

    void *NodeImpl::handle_append_entries_from_cache(void *arg)
    {
        HandleAppendEntriesFromCacheArg *handle_arg = (HandleAppendEntriesFromCacheArg *)arg;
        NodeImpl *node = handle_arg->node;
        butil::LinkedList<AppendEntriesRpc> &rpcs = handle_arg->rpcs;
        while (!rpcs.empty())
        {
            AppendEntriesRpc *rpc = rpcs.head()->value();
            rpc->RemoveFromList();
            node->handle_append_entries_request(rpc->cntl, rpc->request,
                                                rpc->response, rpc->done, true);
            delete rpc;
        }
        node->Release();
        delete handle_arg;
        return NULL;
    }

    void NodeImpl::on_append_entries_cache_timedout(void *arg)
    {
        bthread_t tid;
        if (bthread_start_background(
                &tid, NULL, NodeImpl::handle_append_entries_cache_timedout,
                arg) != 0)
        {
            PLOG(ERROR) << "Fail to start bthread";
            NodeImpl::handle_append_entries_cache_timedout(arg);
        }
    }

    struct AppendEntriesCacheTimerArg
    {
        NodeImpl *node;
        int64_t timer_version;
        int64_t cache_version;
        int64_t timer_start_ms;
    };

    void *NodeImpl::handle_append_entries_cache_timedout(void *arg)
    {
        AppendEntriesCacheTimerArg *timer_arg = (AppendEntriesCacheTimerArg *)arg;
        NodeImpl *node = timer_arg->node;

        std::unique_lock<raft_mutex_t> lck(node->_mutex);
        if (node->_append_entries_cache &&
            timer_arg->cache_version == node->_append_entries_cache->cache_version())
        {
            node->_append_entries_cache->do_handle_append_entries_cache_timedout(
                timer_arg->timer_version, timer_arg->timer_start_ms);
            if (node->_append_entries_cache->empty())
            {
                delete node->_append_entries_cache;
                node->_append_entries_cache = NULL;
            }
        }
        lck.unlock();
        delete timer_arg;
        node->Release();
        return NULL;
    }

    int64_t NodeImpl::AppendEntriesCache::first_index() const
    {
        CHECK(!_rpc_map.empty());
        CHECK(!_rpc_queue.empty());
        return _rpc_map.begin()->second->request->prev_log_index() + 1;
    }

    int64_t NodeImpl::AppendEntriesCache::cache_version() const
    {
        return _cache_version;
    }

    bool NodeImpl::AppendEntriesCache::empty() const
    {
        return _rpc_map.empty();
    }

    bool NodeImpl::AppendEntriesCache::store(AppendEntriesRpc *rpc)
    {
        if (!_rpc_map.empty())
        {
            bool need_clear = false;
            // 第一个index大于等于 rpc->request->prev_log_index()  的位置
            std::map<int64_t, AppendEntriesRpc *>::iterator it =
                _rpc_map.lower_bound(rpc->request->prev_log_index());
            // 当前rpc请求的第一个entry的index和最后一个entry的index
            int64_t rpc_prev_index = rpc->request->prev_log_index();
            int64_t rpc_last_index = rpc_prev_index + rpc->request->entries_size();

            // Some rpcs with the overlap log index alredy exist, means retransmission
            // happend, simplely clean all out of order requests, and store the new
            // one.
            if (it != _rpc_map.begin())
            {
                // 该位置的前一个rpc请求的last_index落到了当前rpc请求的prev_log_index之后
                // 说明当前rpc请求中有一部分entry是重发的，因此需要清理
                // 比如前一个rpc请求包括index[3,4,5]，当前rpc请求的index[4,5,6]
                --it;
                AppendEntriesRpc *prev_rpc = it->second;
                if (prev_rpc->request->prev_log_index() +
                        prev_rpc->request->entries_size() >
                    rpc_prev_index)
                {
                    need_clear = true;
                }
                ++it;
            }
            if (!need_clear && it != _rpc_map.end())
            {
                // 比如前一个rpc请求包括index[3,4,5]，当前rpc请求的index[3,4]
                AppendEntriesRpc *next_rpc = it->second;
                if (next_rpc->request->prev_log_index() < rpc_last_index)
                {
                    need_clear = true;
                }
            }
            // 有重复的entry，需要执行清理操作
            if (need_clear)
            {
                clear();
            }
        }
        // 把当前rpc请求放进缓存
        _rpc_queue.Append(rpc);
        _rpc_map.insert(std::make_pair(rpc->request->prev_log_index(), rpc));

        // The first rpc need to start the timer
        // 第一个rpc到来时启动计时器, 这个是为了防止请求等待时间过长
        if (_rpc_map.size() == 1)
        {
            if (!start_timer())
            {
                clear();
                return true;
            }
        }
        HandleAppendEntriesFromCacheArg *arg = NULL;
        while (_rpc_map.size() > (size_t)FLAGS_raft_max_append_entries_cache_size)
        {
            std::map<int64_t, AppendEntriesRpc *>::iterator it = _rpc_map.end();
            --it;
            AppendEntriesRpc *rpc_to_release = it->second;
            rpc_to_release->RemoveFromList();
            _rpc_map.erase(it);
            if (arg == NULL)
            {
                arg = new HandleAppendEntriesFromCacheArg;
                arg->node = _node;
            }
            arg->rpcs.Append(rpc_to_release);
        }
        if (arg != NULL)
        {
            start_to_handle(arg);
        }
        return true;
    }

    void NodeImpl::AppendEntriesCache::process_runable_rpcs(int64_t local_last_index)
    {
        CHECK(!_rpc_map.empty());
        CHECK(!_rpc_queue.empty());
        HandleAppendEntriesFromCacheArg *arg = NULL;
        for (std::map<int64_t, AppendEntriesRpc *>::iterator it = _rpc_map.begin();
             it != _rpc_map.end();)
        {
            AppendEntriesRpc *rpc = it->second;
            if (rpc->request->prev_log_index() > local_last_index)
            {
                break;
            }
            local_last_index = rpc->request->prev_log_index() + rpc->request->entries_size();
            _rpc_map.erase(it++);
            rpc->RemoveFromList();
            if (arg == NULL)
            {
                arg = new HandleAppendEntriesFromCacheArg;
                arg->node = _node;
            }
            arg->rpcs.Append(rpc);
        }
        if (arg != NULL)
        {
            start_to_handle(arg);
        }
        if (_rpc_map.empty())
        {
            stop_timer();
        }
    }

    void NodeImpl::AppendEntriesCache::clear()
    {
        BRAFT_VLOG << "node " << _node->_group_id << ":" << _node->_server_id
                   << " clear append entries cache";
        stop_timer();
        HandleAppendEntriesFromCacheArg *arg = new HandleAppendEntriesFromCacheArg;
        arg->node = _node;
        // 清空现有缓存
        while (!_rpc_queue.empty())
        {
            AppendEntriesRpc *rpc = _rpc_queue.head()->value();
            rpc->RemoveFromList();
            arg->rpcs.Append(rpc);
        }
        _rpc_map.clear();
        start_to_handle(arg);
    }

    void NodeImpl::AppendEntriesCache::ack_fail(AppendEntriesRpc *rpc)
    {
        rpc->cntl->SetFailed(EINVAL, "Fail to handle out-of-order requests");
        rpc->done->Run();
        delete rpc;
    }

    void NodeImpl::AppendEntriesCache::start_to_handle(HandleAppendEntriesFromCacheArg *arg)
    {
        _node->AddRef();
        bthread_t tid;
        // Sequence if not important
        // 创建cache处理线程处理缓存中的每一个rpc请求
        if (bthread_start_background(
                &tid, NULL, NodeImpl::handle_append_entries_from_cache,
                arg) != 0)
        {
            PLOG(ERROR) << "Fail to start bthread";
            // We cant't call NodeImpl::handle_append_entries_from_cache
            // here since we are in the mutex, which will cause dead lock, just
            // set the rpc fail, and let leader block for a while.
            butil::LinkedList<AppendEntriesRpc> &rpcs = arg->rpcs;
            while (!rpcs.empty())
            {
                AppendEntriesRpc *rpc = rpcs.head()->value();
                rpc->RemoveFromList();
                ack_fail(rpc);
            }
            _node->Release();
            delete arg;
        }
    }

    bool NodeImpl::AppendEntriesCache::start_timer()
    {
        ++_timer_version;
        AppendEntriesCacheTimerArg *timer_arg = new AppendEntriesCacheTimerArg;
        timer_arg->node = _node;
        timer_arg->timer_version = _timer_version;
        timer_arg->cache_version = _cache_version;
        timer_arg->timer_start_ms = _rpc_queue.head()->value()->receive_time_ms;
        timespec duetime = butil::milliseconds_from(
            butil::milliseconds_to_timespec(timer_arg->timer_start_ms),
            std::max(_node->_options.election_timeout_ms >> 2, 1));
        _node->AddRef();
        if (bthread_timer_add(
                &_timer, duetime, NodeImpl::on_append_entries_cache_timedout,
                timer_arg) != 0)
        {
            LOG(ERROR) << "Fail to add timer";
            delete timer_arg;
            _node->Release();
            return false;
        }
        return true;
    }

    void NodeImpl::AppendEntriesCache::stop_timer()
    {
        if (_timer == bthread_timer_t())
        {
            return;
        }
        ++_timer_version;
        if (bthread_timer_del(_timer) == 0)
        {
            _node->Release();
            _timer = bthread_timer_t();
        }
    }

    void NodeImpl::AppendEntriesCache::do_handle_append_entries_cache_timedout(
        int64_t timer_version, int64_t timer_start_ms)
    {
        if (timer_version != _timer_version)
        {
            return;
        }
        CHECK(!_rpc_map.empty());
        CHECK(!_rpc_queue.empty());
        // If the head of out-of-order requests is not be handled, clear the entire cache,
        // otherwise, start a new timer.
        if (_rpc_queue.head()->value()->receive_time_ms <= timer_start_ms)
        {
            clear();
            return;
        }
        if (!start_timer())
        {
            clear();
        }
    }

    void NodeImpl::ConfigurationCtx::start(const Configuration &old_conf,
                                           const Configuration &new_conf,
                                           Closure *done)
    {
        CHECK(!is_busy());
        CHECK(!_done);
        _done = done;
        // 设置当前状态为 STAGE_CATCHING_UP
        _stage = STAGE_CATCHING_UP;
        // 获取新旧配置下的全部节点
        old_conf.list_peers(&_old_peers);
        new_conf.list_peers(&_new_peers);
        Configuration adding;
        Configuration removing;
        // 通过比较新旧配置获取要增加的配置和要删除的配置
        new_conf.diffs(old_conf, &adding, &removing);
        // 变更次数，涉及多少个节点就有多少次变更
        _nchanges = adding.size() + removing.size();

        std::stringstream ss;
        ss << "node " << _node->_group_id << ":" << _node->_server_id
           << " change_peers from " << old_conf << " to " << new_conf;

        // 没有新增节点，说明是删除配置，进入下一个阶段的处理
        if (adding.empty())
        {
            ss << ", begin removing.";
            LOG(INFO) << ss.str();
            return next_stage();
        }

        // 否则需要让新加的节点追赶日志之后才能进入下一个阶段
        ss << ", begin caughtup.";
        LOG(INFO) << ss.str();
        adding.list_peers(&_adding_peers);
        for (std::set<PeerId>::const_iterator iter = _adding_peers.begin(); iter != _adding_peers.end(); ++iter)
        {
            // 为新节点创建并启动replicator并加入到 _replicator_group 中
            if (_node->_replicator_group.add_replicator(*iter) != 0)
            {
                LOG(ERROR) << "node " << _node->node_id()
                           << " start replicator failed, peer " << *iter;
                return on_caughtup(_version, *iter, false);
            }
            //#######
            // 回调函数执行的是 NodeImpl::on_caughtup
            OnCaughtUp *caught_up = new OnCaughtUp(
                _node, _node->_current_term, *iter, _version);
            timespec due_time = butil::milliseconds_from_now(
                _node->_options.get_catchup_timeout_ms());

            // 调用_node->_replicator_group.wait_caughtup，等到新节点的日志追赶成功就调用回调进入下一个stage
            // 如果超时了还没有赶上，并且节点还存活着就重试
            // 是否追赶上的判断标志是新加入节点和leader之间的log index的差距小于catchup_margin, catchup_margin由NodeOption中的catchup_margin变量指定，默认是1000
            // 这里就是等待该节点追赶上leader，追赶上了就执行回调函数
            if (_node->_replicator_group.wait_caughtup(
                    *iter, _node->_options.catchup_margin, &due_time, caught_up) != 0)
            {
                LOG(WARNING) << "node " << _node->node_id()
                             << " wait_caughtup failed, peer " << *iter;
                delete caught_up;
                return on_caughtup(_version, *iter, false);
            }
        }
    }

    void NodeImpl::ConfigurationCtx::flush(const Configuration &conf,
                                           const Configuration &old_conf)
    {
        CHECK(!is_busy());
        conf.list_peers(&_new_peers);
        // 没有旧配置，说明在上一个leader崩溃时没有进行或者已经完成配置更新操作
        if (old_conf.empty())
        {
            _stage = STAGE_STABLE;
            _old_peers = _new_peers;
        }
        // 有旧配置
        else
        {
            _stage = STAGE_JOINT;
            old_conf.list_peers(&_old_peers);
        }
        _node->unsafe_apply_configuration(conf, old_conf.empty() ? NULL : &old_conf,
                                          true);
    }

    void NodeImpl::ConfigurationCtx::on_caughtup(
        int64_t version, const PeerId &peer_id, bool succ)
    {
        if (version != _version)
        {
            LOG(WARNING) << "Node " << _node->node_id()
                         << " on_caughtup with unmatched version=" << version
                         << ", expect version=" << _version;
            return;
        }
        CHECK_EQ(STAGE_CATCHING_UP, _stage);
        if (succ)
        {
            _adding_peers.erase(peer_id);
            if (_adding_peers.empty())
            {
                // 当前配置状态是 STAGE_CATCHING_UP， 修改为 STAGE_JOINT，然后执行unsafe_apply_configuration
                return next_stage();
            }
            return;
        }
        // Fail
        LOG(WARNING) << "Node " << _node->node_id()
                     << " fail to catch up peer " << peer_id
                     << " when trying to change peers from "
                     << Configuration(_old_peers) << " to "
                     << Configuration(_new_peers);
        butil::Status err(ECATCHUP, "Peer %s failed to catch up",
                          peer_id.to_string().c_str());
        reset(&err);
    }

    void NodeImpl::ConfigurationCtx::next_stage()
    {
        CHECK(is_busy());
        switch (_stage)
        {
        case STAGE_CATCHING_UP:
            // _nchanges 表示要配置更新的节点数 ，大于1表示还有节点未处理
            if (_nchanges > 1)
            {
                _stage = STAGE_JOINT;
                Configuration old_conf(_old_peers);
                return _node->unsafe_apply_configuration(
                    Configuration(_new_peers), &old_conf, false);
            }
            // Skip joint consensus since only one peer has been changed here. Make
            // it a one-stage change to be compitible with the legacy
            // implementation.
        case STAGE_JOINT:
            _stage = STAGE_STABLE;
            //  当前处于稳定状态, 将旧配置设置为null, 新来的entry生成投票箱的时候只会在新配置下生成
            //  包含新配置的entry被提交后进入下一个阶段， 即 STAGE_STABLE 阶段
            return _node->unsafe_apply_configuration(
                Configuration(_new_peers), NULL, false);
        case STAGE_STABLE:
        {
            // 检查当前leader是否还在新节点列表中，如果不在就step_down
            bool should_step_down =
                _new_peers.find(_node->_server_id) == _new_peers.end();
            butil::Status st = butil::Status::OK();
            reset(&st);
            // 如果不在新配置中需要进行step_down
            if (should_step_down)
            {
                _node->step_down(_node->_current_term, true,
                                 butil::Status(ELEADERREMOVED, "This node was removed"));
            }
            return;
        }
        case STAGE_NONE:
            CHECK(false) << "Can't reach here";
            return;
        }
    }

    void NodeImpl::ConfigurationCtx::reset(butil::Status *st)
    {
        // reset() should be called only once
        if (_stage == STAGE_NONE)
        {
            BRAFT_VLOG << "node " << _node->node_id()
                       << " reset ConfigurationCtx when stage is STAGE_NONE already";
            return;
        }

        LOG(INFO) << "node " << _node->node_id()
                  << " reset ConfigurationCtx, new_peers: " << Configuration(_new_peers)
                  << ", old_peers: " << Configuration(_old_peers);
        if (st && st->ok())
        {
            _node->stop_replicator(_new_peers, _old_peers);
        }
        else
        {
            // leader step_down may stop replicators of catching up nodes, leading to
            // run catchup_closure
            _node->stop_replicator(_old_peers, _new_peers);
        }
        _new_peers.clear();
        _old_peers.clear();
        _adding_peers.clear();
        ++_version;
        _stage = STAGE_NONE;
        _nchanges = 0;
        _node->check_majority_nodes_readonly();
        if (_done)
        {
            if (!st)
            {
                _done->status().set_error(EPERM, "leader stepped down");
            }
            else
            {
                _done->status() = *st;
            }
            run_closure_in_bthread(_done);
            _done = NULL;
        }
    }

    void NodeImpl::enter_readonly_mode()
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_node_readonly)
        {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " enter readonly mode";
            _node_readonly = true;
        }
    }

    void NodeImpl::leave_readonly_mode()
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_node_readonly)
        {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " leave readonly mode";
            _node_readonly = false;
        }
    }

    bool NodeImpl::readonly()
    {
        BAIDU_SCOPED_LOCK(_mutex);
        return _node_readonly || _majority_nodes_readonly;
    }

    int NodeImpl::change_readonly_config(int64_t term, const PeerId &peer_id, bool readonly)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (term != _current_term && _state != STATE_LEADER)
        {
            return EINVAL;
        }
        _replicator_group.change_readonly_config(peer_id, readonly);
        check_majority_nodes_readonly();
        return 0;
    }

    void NodeImpl::check_majority_nodes_readonly()
    {
        check_majority_nodes_readonly(_conf.conf);
        if (!_conf.old_conf.empty())
        {
            check_majority_nodes_readonly(_conf.old_conf);
        }
    }

    void NodeImpl::check_majority_nodes_readonly(const Configuration &conf)
    {
        std::vector<PeerId> peers;
        conf.list_peers(&peers);
        size_t readonly_nodes = 0;
        for (size_t i = 0; i < peers.size(); i++)
        {
            if (peers[i] == _server_id)
            {
                readonly_nodes += ((_node_readonly) ? 1 : 0);
                continue;
            }
            if (_replicator_group.readonly(peers[i]))
            {
                ++readonly_nodes;
            }
        }
        size_t writable_nodes = peers.size() - readonly_nodes;
        bool prev_readonly = _majority_nodes_readonly;
        _majority_nodes_readonly = !(writable_nodes >= (peers.size() / 2 + 1));
        if (prev_readonly != _majority_nodes_readonly)
        {
            LOG(INFO) << "node " << _group_id << ":" << _server_id
                      << " majority readonly change from " << (prev_readonly ? "enable" : "disable")
                      << " to " << (_majority_nodes_readonly ? " enable" : "disable");
        }
    }

    bool NodeImpl::is_leader_lease_valid()
    {
        LeaderLeaseStatus lease_status;
        get_leader_lease_status(&lease_status);
        return lease_status.state == LEASE_VALID;
    }

    void NodeImpl::get_leader_lease_status(LeaderLeaseStatus *lease_status)
    {
        // Fast path for leader to lease check
        LeaderLease::LeaseInfo internal_info;
        _leader_lease.get_lease_info(&internal_info);
        switch (internal_info.state)
        {
        case LeaderLease::DISABLED:
            lease_status->state = LEASE_DISABLED;
            return;
        case LeaderLease::EXPIRED:
            lease_status->state = LEASE_EXPIRED;
            return;
        case LeaderLease::NOT_READY:
            lease_status->state = LEASE_NOT_READY;
            return;
        case LeaderLease::VALID:
            lease_status->term = internal_info.term;
            lease_status->lease_epoch = internal_info.lease_epoch;
            lease_status->state = LEASE_VALID;
            return;
        case LeaderLease::SUSPECT:
            // Need do heavy check to judge if a lease still valid.
            break;
        }

        BAIDU_SCOPED_LOCK(_mutex);
        if (_state != STATE_LEADER)
        {
            lease_status->state = LEASE_EXPIRED;
            return;
        }
        int64_t last_active_timestamp = last_leader_active_timestamp();
        _leader_lease.renew(last_active_timestamp);
        _leader_lease.get_lease_info(&internal_info);
        if (internal_info.state != LeaderLease::VALID && internal_info.state != LeaderLease::DISABLED)
        {
            butil::Status status;
            status.set_error(ERAFTTIMEDOUT, "Leader lease expired");
            step_down(_current_term, false, status);
            lease_status->state = LEASE_EXPIRED;
        }
        else if (internal_info.state == LeaderLease::VALID)
        {
            lease_status->term = internal_info.term;
            lease_status->lease_epoch = internal_info.lease_epoch;
            lease_status->state = LEASE_VALID;
        }
        else
        {
            lease_status->state = LEASE_DISABLED;
        }
    }

    void NodeImpl::VoteBallotCtx::init(NodeImpl *node, bool triggered)
    {
        reset(node);
        _ballot.init(node->_conf.conf, node->_conf.stable() ? NULL : &(node->_conf.old_conf));
        _triggered = triggered;
    }

    void NodeImpl::VoteBallotCtx::start_grant_self_timer(int64_t wait_ms, NodeImpl *node)
    {
        timespec duetime = butil::milliseconds_from_now(wait_ms);
        GrantSelfArg *timer_arg = new GrantSelfArg;
        timer_arg->node = node;
        timer_arg->vote_ctx_version = _version;
        timer_arg->vote_ctx = this;
        node->AddRef();
        _grant_self_arg = timer_arg;
        if (bthread_timer_add(
                &_timer, duetime, NodeImpl::on_grant_self_timedout,
                timer_arg) != 0)
        {
            LOG(ERROR) << "Fail to add timer";
            delete timer_arg;
            _grant_self_arg = NULL;
            node->Release();
        }
    }

    void NodeImpl::VoteBallotCtx::stop_grant_self_timer(NodeImpl *node)
    {
        if (_timer == bthread_timer_t())
        {
            return;
        }
        if (bthread_timer_del(_timer) == 0)
        {
            node->Release();
            delete _grant_self_arg;
            _grant_self_arg = NULL;
            _timer = bthread_timer_t();
        }
    }

    void NodeImpl::VoteBallotCtx::reset(NodeImpl *node)
    {
        stop_grant_self_timer(node);
        ++_version;
        _triggered = false;
        _last_log_id = LogId();
        _disrupted_leader = DisruptedLeader();
        _reserved_peers.clear();
    }

    void NodeImpl::VoteBallotCtx::reserve(const PeerId &peer)
    {
        _reserved_peers.insert(peer);
    }

    void NodeImpl::VoteBallotCtx::set_disrupted_leader(const DisruptedLeader &peer)
    {
        _disrupted_leader = peer;
    }

    const NodeImpl::DisruptedLeader &NodeImpl::VoteBallotCtx::disrupted_leader() const
    {
        return _disrupted_leader;
    }

    void NodeImpl::VoteBallotCtx::pop_grantable_peers(std::set<PeerId> *peers)
    {
        peers->clear();
        if (_disrupted_leader.term == -1)
        {
            return;
        }
        _reserved_peers.swap(*peers);
    }

    void NodeImpl::VoteBallotCtx::set_last_log_id(const LogId &log_id)
    {
        _last_log_id = log_id;
    }

    const LogId &NodeImpl::VoteBallotCtx::last_log_id() const
    {
        return _last_log_id;
    }

    void NodeImpl::grant_self(VoteBallotCtx *vote_ctx, std::unique_lock<raft_mutex_t> *lck)
    {
        // If follower lease expired, we can safely grant self. Otherwise, we wait util:
        // 1. last active leader vote the node, and we grant two votes together;
        // 2. follower lease expire.

        // votable_time_from_now是计算当前时间与租约截止时间的差值，返回0表示已经过了租期，这时候换主不会导致脑裂
        // 不为0时表示当前还有多久租期才过，需要等待wait_ms
        int64_t wait_ms = _follower_lease.votable_time_from_now();
        if (wait_ms == 0)
        {
            // 给自己投一票
            vote_ctx->grant(_server_id);
            // granted()判断Ballox对象的法定票数是否减到0，如果减到0说明已经或得了过半数的票数
            // 否则就说明拉选票失败，后面就不用执行了
            if (!vote_ctx->granted())
            {
                return;
            }
            // 当前是预投票
            if (vote_ctx == &_pre_vote_ctx)
            {
                elect_self(lck);
            }
            // 当前是正式投票
            else
            {
                become_leader();
            }
            return;
        }
        // 等待wait_ms时间才能开始执行 grant_self
        vote_ctx->start_grant_self_timer(wait_ms, this);
    }

    void NodeImpl::on_grant_self_timedout(void *arg)
    {
        bthread_t tid;
        if (bthread_start_background(
                &tid, NULL, NodeImpl::handle_grant_self_timedout,
                arg) != 0)
        {
            PLOG(ERROR) << "Fail to start bthread";
            NodeImpl::handle_grant_self_timedout(arg);
        }
    }

    void *NodeImpl::handle_grant_self_timedout(void *arg)
    {
        GrantSelfArg *grant_arg = (GrantSelfArg *)arg;
        NodeImpl *node = grant_arg->node;
        VoteBallotCtx *vote_ctx = grant_arg->vote_ctx;
        int64_t vote_ctx_version = grant_arg->vote_ctx_version;

        delete grant_arg;

        std::unique_lock<raft_mutex_t> lck(node->_mutex);
        if (!is_active_state(node->_state) ||
            vote_ctx->version() != vote_ctx_version)
        {
            lck.unlock();
            node->Release();
            return NULL;
        }
        node->grant_self(vote_ctx, &lck);
        lck.unlock();
        node->Release();
        return NULL;
    }

    void NodeImpl::leader_lease_start(int64_t lease_epoch)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_state == STATE_LEADER)
        {
            _leader_lease.on_lease_start(
                lease_epoch, last_leader_active_timestamp());
        }
    }

    int64_t NodeImpl::last_leader_active_timestamp()
    {
        int64_t timestamp = last_leader_active_timestamp(_conf.conf);
        if (!_conf.old_conf.empty())
        {
            timestamp = std::min(timestamp, last_leader_active_timestamp(_conf.old_conf));
        }
        return timestamp;
    }

    struct LastActiveTimestampCompare
    {
        bool operator()(const int64_t &a, const int64_t &b)
        {
            return a > b;
        }
    };

    int64_t NodeImpl::last_leader_active_timestamp(const Configuration &conf)
    {
        std::vector<PeerId> peers;
        conf.list_peers(&peers);
        std::vector<int64_t> last_rpc_send_timestamps;
        LastActiveTimestampCompare compare;
        for (size_t i = 0; i < peers.size(); i++)
        {
            if (peers[i] == _server_id)
            {
                continue;
            }

            int64_t timestamp = _replicator_group.last_rpc_send_timestamp(peers[i]);
            last_rpc_send_timestamps.push_back(timestamp);
            std::push_heap(last_rpc_send_timestamps.begin(), last_rpc_send_timestamps.end(), compare);
            if (last_rpc_send_timestamps.size() > peers.size() / 2)
            {
                std::pop_heap(last_rpc_send_timestamps.begin(), last_rpc_send_timestamps.end(), compare);
                last_rpc_send_timestamps.pop_back();
            }
        }
        // Only one peer in the group.
        if (last_rpc_send_timestamps.empty())
        {
            return butil::monotonic_time_ms();
        }
        std::pop_heap(last_rpc_send_timestamps.begin(), last_rpc_send_timestamps.end(), compare);
        return last_rpc_send_timestamps.back();
    }

    // Timers
    int NodeTimer::init(NodeImpl *node, int timeout_ms)
    {
        BRAFT_RETURN_IF(RepeatedTimerTask::init(timeout_ms) != 0, -1);
        _node = node;
        node->AddRef();
        return 0;
    }

    void NodeTimer::on_destroy()
    {
        if (_node)
        {
            _node->Release();
            _node = NULL;
        }
    }

    void ElectionTimer::run()
    {
        _node->handle_election_timeout();
    }

    int ElectionTimer::adjust_timeout_ms(int timeout_ms)
    {
        return random_timeout(timeout_ms);
    }

    void VoteTimer::run()
    {
        _node->handle_vote_timeout();
    }

    int VoteTimer::adjust_timeout_ms(int timeout_ms)
    {
        return random_timeout(timeout_ms);
    }

    void StepdownTimer::run()
    {
        _node->handle_stepdown_timeout();
    }

    void SnapshotTimer::run()
    {
        _node->handle_snapshot_timeout();
    }

} //  namespace braft
