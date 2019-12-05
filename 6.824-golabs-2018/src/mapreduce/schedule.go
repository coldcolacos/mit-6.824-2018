package mapreduce

import (
    "fmt"
    "log"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func work(task_id int, finish_list chan int, task_args DoTaskArgs, registerChan chan string){
	success := false
	for success == false {
		worker := <-registerChan
		success = call(worker, "Worker.DoTask", task_args, nil)
		if(success){
			finish_list <- task_id
			registerChan <- worker
		}
	}
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	/*** lab 1, part 3/4 ***/

	finish_list := make(chan int, 0)
	// finish_list := make(chan int, n_tasks) // slower

	for i := 0; i < ntasks; i ++ {
		task_args := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
		go work(i, finish_list, task_args, registerChan)
	}

	for i := 0; i < ntasks; i ++ {
		task_id := <-finish_list
		log.Printf("Task %d finished.\n", task_id)
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
