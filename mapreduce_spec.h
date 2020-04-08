#pragma once

#include <assert.h>
#include <string.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config
 * file */
struct MapReduceSpec {
  int workerNums;                     // # of workers
  int outputNums;                     // # of output files
  int mapSize;                        // map kilobytes
  std::string outputDir;                 // output directory
  std::string userId;                    // user id
  std::vector<std::string> workerAddrs;  // worker addresses
  std::vector<std::string> inputFiles;   // input files
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification
 * from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
  std::ifstream myfile(config_filename);
  std::unordered_map<std::string, std::vector<std::string>> map;
  
  if (myfile.is_open()) {
    std::string line;
    while (getline(myfile, line)) {
			istringstream is_line(line);
				string key;
			if (getline(is_line, key, '=')) {
				string value;
				if (!getline(is_line, value)) break;
					vector<string> records;
					istringstream is_value(value);
					while (is_value) {
						string record;
						if (!getline(is_value, record, ',')) break;
						records.push_back(record);
					}
					map[key] = records;
				}
			}
    myfile.close();
  } else {
    std::cerr << "Failed to open file " << config_filename << std::endl;
    return false;
  }

  // read specifications from conf file and store them into structure mr_spec
  mr_spec.workerNums = atoi(map["n_workers"][0].c_str());
  mr_spec.outputNums = atoi(map["n_output_files"][0].c_str());
  mr_spec.mapSize = atoi(map["map_kilobytes"][0].c_str());

  mr_spec.outputDir = map["output_dir"][0];
  mr_spec.userId = map["user_id"][0];
 
  mr_spec.workerAddrs = std::move(map["worker_ipaddr_ports"]);
  mr_spec.inputFiles = std::move(map["input_files"]);

  return true;
}

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
  bool ret_val = true;
  
  if (mr_spec.outputNums <= 0) {
		cerr << "[ERROR] in Spec: invalid number of output files." << endl;
		ret_val = false;
	}
  if (mr_spec.mapSize <= 0) {
		cerr << "[ERROR] in Spec: invalid map size." << endl;
		ret_val = false;
	}
	if (mr_spec.workerNums <= 0 && mr_spec.workerNums != mr_spec.workerAddrs.size()) {
		cerr << "[ERROR] in Spec: invalid number of workers." << endl;
		ret_val = false;
	}
  if(mr_spec.userId == "" || mr_spec.outputDir == "") {
  	cerr << "[ERROR] in Spec: user id and ourput DIR undefined." << endl;
		ret_val = false;
	}
  
  
  // validate input file path

  fstream file;
  for(int i=0; i < mr_spec.inputFiles.size(); i++) {
		file.open(mr_spec.inputFiles[i], fstream::in);
		if(file.good() != 1) {
			std::cerr << "Error. Can't open input file" << std::endl;
			ret_val = false;
		}
		file.close();
	}

  // validate worker address port
  if(mr_spec.workerAddrs.size() < 1) {
		std::cerr << "invalid port" << std::endl;
		ret_val = false;
	}

  if(mr_spec.workerAddrs.size() != mr_spec.workerNums) {
		std::cerr << "Number of workers not equal to number of worker ipaddresses" << std::endl;
		ret_val = false;
	}

  char hostName[50];
  char port[50];
  for (auto& addr : mr_spec.workerAddrs) {
    sscanf(addr.c_str(), "%[a-z,]:%[0-9]", hostName, port);
    assert(strncmp(hostName, "localhost", 9) == 0);
	  // validate worker address port
  	 if(mr_spec.workerAddrs.size() < 1) {
		std::cerr << "invalid port" << std::endl;
		ret_val = false;
	}
  }

  DIR* dir = opendir(mr_spec.outputDir.c_str());
	if(ENOENT == errno) {
		std::cerr << "Output directory doesn't exist" << std::endl;
		ret_val = false;
	}

  return ret_val;
}
