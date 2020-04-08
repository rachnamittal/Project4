#pragma once

#include <climits>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <cmath>
#include <math.h>
#include <vector>
#include "mapreduce_spec.h"

using namespace std;

//#define FILE_NAME_MAX_LEN 100

/* CS6210_TASK: Create your own data structure here, where you can hold
   information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks
   to the workers for mapping */
//File shard is hash of filenames with start and end offset of data structure in file split
/*
struct FileShard {
  unordered_map<string, 
  pair<streampos, streampos>> shardsMap;
};
*/

struct FileShard {
	std::vector<std::string> shardsMap;
	std::vector<int> begin;
	std::vector<int> end;
};


 inline bool shard_files(const MapReduceSpec& mr_spec,
                        std::vector<FileShard>& fileShards) {

  std::cout << "Rachna Entering into Shard files" << std::endl;
  uint64_t totalSize = 0;   
  std::vector<string> inputs = mr_spec.inputFiles;
                      
  //for (auto& input : mr_spec.inputFiles) {
  for (int j = 0; j < inputs.size(); j++) {
    std::ifstream myfile(inputs[j], std::ios::in | std::ios::ate);
   
	  std::vector<int> sizeofEachFile;
    
    if(!myfile.is_open()){
			std::cout << "Error in opening file\n";
		}
    //sizeofEachFile.push_back(myfile.tellg()); //file_size is in bytes
    //std::cout << "\n sizeofEachFile : " <<sizeofEachFile[j] << std::endl;
    myfile.seekg(0, std::ios::beg);
    streampos begin = myfile.tellg();
    
    myfile.seekg(0, std::ios::end);
    streampos end = myfile.tellg();
    uint64_t Size = (end - begin + 1); //each input file size
    sizeofEachFile.push_back(Size); //file_size is in bytes
    std::cout << "sizeofEachFile : " << sizeofEachFile[j] << endl;
    
    totalSize += (end - begin + 1); //commulative file size of all input files

    std::cout << "\n TotalSize of each file : " <<totalSize << std::endl;
    myfile.close();
  }


 
  //calculate number of shards
  int shardNums = (int) ceil(totalSize / (mr_spec.mapSize * 1024.0)) + 1;
  fileShards.reserve(shardNums);
  std::cout << "\n # of shard are : " <<shardNums << std::endl;

  //Create individual file shard
  //for (auto& input : mr_spec.inputFiles) {
  for (int j = 0; j < inputs.size(); j++) {
    std::ifstream myfile(inputs[j], std::ios::binary);


  //each input file size
  myfile.seekg(0, std::ios::beg);
  std::streampos begin = myfile.tellg();
    cout << "begin " << begin << endl;

  myfile.seekg(0, std::ios::end);
  std::streampos end = myfile.tellg();
  cout << "tellg of end : " << end << endl;
  
  uint64_t fileSize = (end - begin + 1); //each input file size
  cout << "fileSize : " << fileSize << endl;

    std::cout << "\n RACHNA Split filename : " << inputs[j] << " size:" << fileSize
              << ".\n";
    
    //offset zero in beginning
    std::streampos offset = 0;
    uint64_t restSize = fileSize;
    cout << "restSize in beginning: " << restSize << endl;
    while (restSize > 0) {
      
      // find offset begin for a shard
      myfile.seekg(offset, std::ios::beg);
      std::streampos begin = myfile.tellg();
      cout << "begin in while loop" << begin << endl;
      
      // find offset end for a shard
      cout << "spec size: " << mr_spec.mapSize*1024 << endl;
      myfile.seekg(mr_spec.mapSize * 1024, std::ios::cur);
      std::streampos end = myfile.tellg();
      cout << "end in while: " << end << endl;

      // if offset exceed size, set its end position
      if (end >= fileSize) {
        myfile.seekg(0, std::ios::end);
         end = myfile.tellg();
         //shard size
      cout << "end in if: " << end << endl;
       restSize -= (end - begin + 1);
       cout << "restSize in if : " << restSize << endl;
      
      } else {
        // find closest '\n' delimit
        myfile.ignore(LONG_MAX, '\n');
        // seek back to last character in the prev line.
				myfile.seekg(-2, ios::cur);
        end = myfile.tellg();
         // include the newline character.
				restSize -= (end - begin + 2);
        cout << "restSize in else : " << restSize << endl;
        // seek to new line
				//myfile.seekg(2, ios::cur);

      }
  
      //shard size
   
      std::cout << "RACHNA-vector Process offset (" << begin << "," << end << ") "
                << restSize << " bytes into shard ...\n";

      // store chunk into shards
      /*
      FileShard temp;
      temp.shardsMap[input] = make_pair(begin, end);
      fileShards.push_back(std::move(temp));
      */

      FileShard shard;
      shard.shardsMap.push_back(inputs[j]);
			shard.begin.push_back(begin);
			shard.end.push_back(end);
      fileShards.push_back(shard);
			
  
      offset = static_cast<int>(end) + 1;
      cout << "offset at end: " << offset << endl;
    }
    myfile.close();
  }

  return true;
}
