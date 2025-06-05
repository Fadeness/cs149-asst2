#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), stop{false} {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 0; i < num_threads; ++i)
    {
        workers.emplace_back([this]() {
            while (true)
            {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    condition.wait(lock, [this] {
                        return stop || !tasks.empty();
                    });

                    if (stop && tasks.empty()) 
                        return;
                    
                    task = std::move(tasks.front());
                    tasks.pop();
                }

                task();
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }

    condition.notify_all();

    for (auto& worker : workers)
    {
        if (worker.joinable())
            worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::vector<std::future<void>>futures;
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; ++i)
        {
            auto task = std::make_shared<std::packaged_task<void()>>(
                [runnable, i, num_total_tasks]() {
                    runnable->runTask(i, num_total_tasks);
                }
            );
            futures.push_back(task->get_future());
            tasks.emplace([task]() { (*task)(); });
        }
    }

    condition.notify_all();

    for (auto& future : futures) 
        future.wait();   
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    //
    // TODO: CS149 students will implement this method in Part B.
    //
    std::unique_lock<std::mutex> lock {graph_mutex};
    int launch_id {launch_id_counter++};
    ++num_unfinished_tasks;
    
    TaskNode taskNode = TaskNode(deps.size());
    taskNode.unfinished_tasks = num_total_tasks;
    task_graph.emplace(launch_id, taskNode);
    
    for (auto& dep : deps)
    {
        task_graph[launch_id].deps.push_back(dep);
    }
    
    for (int i = 0; i < num_total_tasks; ++i)
    {
        auto task = std::make_shared<std::packaged_task<void()>>(
            [this, runnable, launch_id, num_total_tasks, i]() {   
                std::unique_lock<std::mutex> lock {graph_mutex};
                
                condition.wait(lock, [this, launch_id] {
                    uint32_t num_finished_deps {0};
                    for (auto& dep : task_graph[launch_id].deps)
                    {
                        if (task_graph[dep].enqueued)
                            ++num_finished_deps;
                    }
                    return num_finished_deps == task_graph[launch_id].deps.size();
                });

                runnable->runTask(i, num_total_tasks);
                if (--task_graph[launch_id].unfinished_tasks == 0)
                {
                    task_graph[launch_id].enqueued = true;
                    --num_unfinished_tasks;
                    condition.notify_all();
                }

                lock.unlock();
                condition.notify_all();
            }
        );
        graph_futures.push_back(task->get_future());
        tasks.emplace([task]() { (*task)(); });

    }

    condition.notify_all();

    
    return launch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    while (true)
    {
        std::unique_lock<std::mutex> lock(graph_mutex);
        if (num_unfinished_tasks == 0) {
            break;
        }
        lock.unlock();
        std::this_thread::yield();
    }

    std::unique_lock<std::mutex> lock(graph_mutex);
    for (auto& future : graph_futures) 
        future.wait();  
}
