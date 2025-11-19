#include "indexed_priority_queue.h"

int main()
{
	// test round A (basic insertion)
	IPQ<int,double> ipqA;
	ipqA.insert(30,0);
	ipqA.insert(29,10);
	ipqA.insert(290,1);
	ipqA.insert(5,2);

	std::vector<std::pair<int,int>> heapA;
	while(ipqA.size())
	{
		auto x = ipqA.pop();
		heapA.push_back(x);
	}

	std::vector<std::pair<int,int>> expectedA = {
		{30,0},
		{290,1},
		{5,2},
		{29,10}
	};

	assert(heapA == expectedA);

	// test round B (insertion, deletion and template params)
	IPQ<std::string,int> ipqB;
	ipqB.insert("F#",0);
	ipqB.insert("",0);
	ipqB.insert("Ron0Studios",10);
	ipqB.insert("c++",1);
	ipqB.insert("python",3);
	ipqB.remove("random");
	ipqB.remove("c++");

	std::vector<std::pair<std::string,int>> heapB;
	while(ipqB.size())
	{
		auto x = ipqB.pop();
		heapB.push_back(x);
	}

	std::vector<std::pair<std::string,int>> expectedB = {
		{"F#",0},
		{"",0},
		{"python",3},
		{"Ron0Studios",10}
	};

	assert(heapB == expectedB);

	// test round C (update existing priorities: decrease/increase/no-op and missing key)
	IPQ<int,int> ipqC;
	ipqC.insert(1,5);
	ipqC.insert(2,3);
	ipqC.insert(3,7);

	// decrease priority (higher priority since this is a min-heap)
	ipqC.update(3,1);
	// increase priority
	ipqC.update(2,8);
	// no-op for missing key
	ipqC.update(42,9);
	// no-op equal value
	ipqC.update(1,5);

	std::vector<std::pair<int,int>> heapC;
	while(ipqC.size())
	{
		auto x = ipqC.pop();
		heapC.push_back(x);
	}

	std::vector<std::pair<int,int>> expectedC = {
		{3,1},
		{1,5},
		{2,8}
	};

	assert(heapC == expectedC);
	// test round D (descending by value using max-heap comparator)
	IPQ<int,int,std::greater<int>> ipqD; // max-heap
	ipqD.insert(1,5);
	ipqD.insert(2,3);
	ipqD.insert(3,7);
	// update to change ordering
	ipqD.update(2,9); // now should be the highest
	ipqD.update(1,4); // lower than 7 and 9

	std::vector<std::pair<int,int>> heapD;
	while(ipqD.size())
	{
		auto x = ipqD.pop();
		heapD.push_back(x);
	}

	std::vector<std::pair<int,int>> expectedD = {
		{2,9},
		{3,7},
		{1,4}
	};
	assert(heapD == expectedD);
	return 0;
}