
#ifndef SFWQUERY_H
#define SFWQUERY_H

#include "ExprTree.h"
#include "MyDB_LogicalOps.h"
#include <limits>
#include <set>
#include <unordered_map>

// structure that stores an entire SFW query
struct SFWQuery {

private:

	// the various parts of the SQL query
	vector <ExprTreePtr> valuesToSelect;
	vector <pair <string, string>> tablesToProcess;
	vector <ExprTreePtr> allDisjunctions;
	vector <ExprTreePtr> groupingClauses;

	MyDB_SchemaPtr buildAggSchema (LogicalOpPtr joinOpPtr);
	
	map <string, LogicalOpPtr> getAllTableScan (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters);

	LogicalOpPtr joinTwoTable (LogicalOpPtr lOpPtr, LogicalOpPtr rOpPtr, 
		vector <pair <string, string>> LtableNameAlias, vector <pair <string, string>> RtableNameAlias, bool finalJoin);

	pair <double, MyDB_StatsPtr> optimize (map <string, LogicalOpPtr> scanOpPtrs, map <vector <string>, pair <double, MyDB_StatsPtr>> &optimizedSet);

public:
	SFWQuery () {};

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf, struct ValueList *grouping);

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf);

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause);
	
	// builds and optimizes a logical query plan for a SFW query, returning the resulting logical query plan
	//
	// allTables: this is the list of all of the tables currently in the system
	// allTableReaderWriters: this is so we can store the info that we need to be able to execute the query
	LogicalOpPtr buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, 
		map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters);

	~SFWQuery () {}

	int optimizedCnt = 0;
	void print ();

	#include "FriendDecls.h"
};

#endif
