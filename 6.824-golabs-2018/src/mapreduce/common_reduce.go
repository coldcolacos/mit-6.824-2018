package mapreduce

import (
	"os"
	"log"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	/*** lab 1, part 1 ***/
	key_values := make(map[string] ([]string), 0)

	for i := 0; i < nMap; i ++ {
		fname := reduceName(jobName, i, reduceTask)
		fin, err := os.Open(fname)
		if err != nil {
			log.Println("[doReduce() open file] ", err)
		}
		decoder := json.NewDecoder(fin)

		for decoder.More() {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if(err != nil){
				log.Println("[doReduce() decode] ", err)
			}

			values, exists := key_values[kv.Key]
			if(exists){
				key_values[kv.Key] = append(values, kv.Value)
			} else{
				key_values[kv.Key] = []string {kv.Value}
			}
		}

		fin.Close()
	}

	fout, err := os.Create(outFile)
	if(err != nil){
		log.Println("[doReduce() create file] ", err)
	}

	encoder := json.NewEncoder(fout)
	for k, v := range key_values {
		err := encoder.Encode(KeyValue{k, reduceF(k, v)})
		if(err != nil){
			log.Println("[doReduce() encode] ", err)
		}
	}
	fout.Close()
}
