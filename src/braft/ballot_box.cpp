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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include <butil/scoped_lock.h>
#include <bvar/latency_recorder.h>
#include <bthread/unstable.h>
#include "braft/ballot_box.h"
#include "braft/util.h"
#include "braft/fsm_caller.h"
#include "braft/closure_queue.h"

namespace braft
{

    BallotBox::BallotBox()
        : _waiter(NULL), _closure_queue(NULL), _last_committed_index(0), _pending_index(0)
    {
    }

    BallotBox::~BallotBox()
    {
        clear_pending_tasks();
    }

    int BallotBox::init(const BallotBoxOptions &options)
    {
        if (options.waiter == NULL || options.closure_queue == NULL)
        {
            LOG(ERROR) << "waiter is NULL";
            return EINVAL;
        }
        _waiter = options.waiter;
        _closure_queue = options.closure_queue;
        return 0;
    }

    int BallotBox::commit_at(
        int64_t first_log_index, int64_t last_log_index, const PeerId &peer)
    {
        // FIXME(chenzhangyi01): The cricital section is unacceptable because it
        // blocks all the other Replicators and LogManagers
        std::unique_lock<raft_mutex_t> lck(_mutex);
        if (_pending_index == 0)
        {
            return EINVAL;
        }
        if (last_log_index < _pending_index)
        {
            return 0;
        }
        if (last_log_index >= _pending_index + (int64_t)_pending_meta_queue.size())
        {
            return ERANGE;
        }

        int64_t last_committed_index = 0;
        const int64_t start_at = std::max(_pending_index, first_log_index);
        Ballot::PosHint pos_hint;

        // 检查每一个选票，如果收到了大半follower节点的投票则将其对应的log entry标记为已提交
        for (int64_t log_index = start_at; log_index <= last_log_index; ++log_index)
        {
            // 从 _pending_meta_queue 中取出Ballot
            Ballot &bl = _pending_meta_queue[log_index - _pending_index];
            // 调用 grant 将 quoroum 减1， 表示投了一票
            pos_hint = bl.grant(peer, pos_hint);
            // quoroum和_old_quorum初始值为新旧配置下的半数节点数加1
            // 只要quoroum == 0或者_old_quorum == 0之一满足表示获得了过半数的投票，表示commit了

            if (bl.granted())
            {
                last_committed_index = log_index;
            }
        }

        if (last_committed_index == 0)
        {
            return 0;
        }

        // When removing a peer off the raft group which contains even number of
        // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
        // this case, the log after removal may be committed before some previous
        // logs, since we use the new configuration to deal the quorum of the
        // removal request, we think it's safe to commit all the uncommitted
        // previous logs, which is not well proved right now
        // TODO: add vlog when committing previous logs

        // 已经commit过的就可以从票箱中删除了
        for (int64_t index = _pending_index; index <= last_committed_index; ++index)
        {
            _pending_meta_queue.pop_front();
        }

        // 更新下一个待处理的投票
        _pending_index = last_committed_index + 1;
        // 更新已经commit的投票的位置
        _last_committed_index.store(last_committed_index, butil::memory_order_relaxed);
        lck.unlock();
        // The order doesn't matter
        // 调用 FsmCaller 的 on_committed 方法执行状态机的操作, 即apply
        // 构造一个任务提交到execution_queue里面，最后调用FSMCaller::do_committed去调用用户传入的自定义的StateMachine::on_apply函数执行状态机的操作
        _waiter->on_committed(last_committed_index);
        return 0;
    }

    int BallotBox::clear_pending_tasks()
    {
        std::deque<Ballot> saved_meta;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            saved_meta.swap(_pending_meta_queue);
            _pending_index = 0;
        }
        _closure_queue->clear();
        return 0;
    }

    // 重新指定下一个处理的位置
    int BallotBox::reset_pending_index(int64_t new_pending_index)
    {
        BAIDU_SCOPED_LOCK(_mutex);
        CHECK(_pending_index == 0 && _pending_meta_queue.empty())
            << "pending_index " << _pending_index << " pending_meta_queue "
            << _pending_meta_queue.size();
        CHECK_GT(new_pending_index, _last_committed_index.load(
                                        butil::memory_order_relaxed));
        _pending_index = new_pending_index;
        _closure_queue->reset_first_index(new_pending_index);
        return 0;
    }

    // 添加一个待投票的任务
    int BallotBox::append_pending_task(const Configuration &conf, const Configuration *old_conf,
                                       Closure *closure)
    {
        Ballot bl;
        if (bl.init(conf, old_conf) != 0)
        {
            CHECK(false) << "Fail to init ballot";
            return -1;
        }

        BAIDU_SCOPED_LOCK(_mutex);
        CHECK(_pending_index > 0);
        _pending_meta_queue.push_back(Ballot());
        _pending_meta_queue.back().swap(bl);
        _closure_queue->append_pending_closure(closure);
        return 0;
    }

    // 设置commit日志的位置，只能向后
    int BallotBox::set_last_committed_index(int64_t last_committed_index)
    {
        // FIXME: it seems that lock is not necessary here
        std::unique_lock<raft_mutex_t> lck(_mutex);
        if (_pending_index != 0 || !_pending_meta_queue.empty())
        {
            CHECK(last_committed_index < _pending_index)
                << "node changes to leader, pending_index=" << _pending_index
                << ", parameter last_committed_index=" << last_committed_index;
            return -1;
        }
        // last_committed_index指定的commit位置不能比当前记录的commit位置小
        if (last_committed_index <
            _last_committed_index.load(butil::memory_order_relaxed))
        {
            return EINVAL;
        }
        if (last_committed_index > _last_committed_index.load(butil::memory_order_relaxed))
        {
            _last_committed_index.store(last_committed_index, butil::memory_order_relaxed);
            lck.unlock();
            _waiter->on_committed(last_committed_index);
        }
        return 0;
    }

    void BallotBox::describe(std::ostream &os, bool use_html)
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        int64_t committed_index = _last_committed_index;
        int64_t pending_index = 0;
        size_t pending_queue_size = 0;
        if (_pending_index != 0)
        {
            pending_index = _pending_index;
            pending_queue_size = _pending_meta_queue.size();
        }
        lck.unlock();
        const char *newline = use_html ? "<br>" : "\r\n";
        os << "last_committed_index: " << committed_index << newline;
        if (pending_queue_size != 0)
        {
            os << "pending_index: " << pending_index << newline;
            os << "pending_queue_size: " << pending_queue_size << newline;
        }
    }

    //获取投票箱的状态
    void BallotBox::get_status(BallotBoxStatus *status)
    {
        if (!status)
        {
            return;
        }
        std::unique_lock<raft_mutex_t> lck(_mutex);
        status->committed_index = _last_committed_index;
        if (_pending_meta_queue.size() != 0)
        {
            status->pending_index = _pending_index;
            status->pending_queue_size = _pending_meta_queue.size();
        }
    }

} //  namespace braft
