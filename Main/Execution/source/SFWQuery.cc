
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
// 
// note that this implementation only works for two-table queries that do not have an aggregation
// 

MyDB_SchemaPtr SFWQuery :: buildAggSchema (vector <pair <string, MyDB_AttTypePtr>> joinSchemaAtts, vector <string> tableAlias) {
	map <ExprTreePtr, bool> selectForGrouping;
	MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();
			
	for (auto g : groupingClauses) {
		bool findAttr = false;
		for (auto a: valuesToSelect) {
			for (auto alias : tableAlias) {
				for (auto b: joinSchemaAtts) {
					if ((a->referencesAtt (alias, b.first) && g->referencesAtt (alias, b.first)) ||
						(g->getId() == b.first)) {
						findAttr = true;
						selectForGrouping[a] = true;
						aggSchema->getAtts ().push_back (make_pair (alias+ "_" + b.first, b.second));
						break;
					}
				}

				if (findAttr) break;
			}

			if (findAttr) break;
		}
	} 

	int i = 0;

	for (auto a: valuesToSelect) {
		if (selectForGrouping[a]) {
			continue;
		}

		bool constant = true;
		bool find = false;

		for (auto b: joinSchemaAtts) {
			for (auto alias : tableAlias) {
				if (a->referencesAtt (alias, b.first)) {
					constant = false;
					find = true;
					aggSchema->getAtts ().push_back (make_pair (alias+ "_" + b.first, b.second));
					break;
				}
			}

			if (find) break;
		}

		if (constant && !find) {
			cout << "Constant for select: "<< a->toString() << endl;

			if (a->toString().find("int") != 0) {
				aggSchema->getAtts ().push_back (make_pair ("consant_" + to_string (i++), make_shared <MyDB_IntAttType> ()));
			} else if (a->toString().find("double") != 0) {
				aggSchema->getAtts ().push_back (make_pair ("consant_" + to_string (i++), make_shared <MyDB_DoubleAttType> ()));
			} else if (a->toString().find("string") != 0) {
				aggSchema->getAtts ().push_back (make_pair ("consant_" + to_string (i++), make_shared <MyDB_StringAttType> ()));
			} else if (a->toString().find("bool") != 0) {
				aggSchema->getAtts ().push_back (make_pair ("consant_" + to_string (i++), make_shared <MyDB_BoolAttType> ()));
			}
		}
	}

	return aggSchema;
}

vector <LogicalOpPtr> SFWQuery :: getAllTableScan (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {
	vector <LogicalOpPtr> opPtrs;
	vector <ExprTreePtr> topCNF; 

	for (auto a: allDisjunctions) {
		topCNF.push_back (a);
	}

	for (int i = 0; i < tablesToProcess.size(); i++) {
		vector <string> exprs;
		vector <ExprTreePtr> CNF;
		MyDB_SchemaPtr schema = make_shared <MyDB_Schema> ();

		string tableAlias = tablesToProcess[i].second;
		string tableName = tablesToProcess[i].first;

		MyDB_TableReaderWriterPtr tableRwPtr = allTableReaderWriters[tableName];
		
		for (auto a: allDisjunctions) {
			bool needOtherTable = false;
			for (auto nameAlias : tablesToProcess) {
				if (nameAlias.first == tableName) {
					continue;
				}

				if (a->referencesTable(nameAlias.second)) {
					needOtherTable = true;
					break;
				}

				if (needOtherTable) break;
			}

			if (!needOtherTable) CNF.push_back(a);
		}
		cout << "Table " << tableName << endl;
		// and see what we need from the left, and from the right
		for (auto b: tableRwPtr->getTable()->getSchema ()->getAtts ()) {
			bool needIt = false;
			string alias; 
			for (auto a: valuesToSelect) {
				if (a->referencesAtt (tableAlias, b.first)) {
					needIt = true;
					break;
				}
			
			}
			for (auto a: topCNF) {
				if (a->referencesAtt (tableAlias, b.first)) {
					needIt = true;
					break;
				}
			}

			if (needIt) {
				schema->getAtts ().push_back (make_pair (tableAlias + "_" + b.first, b.second));
				exprs.push_back ("[" + b.first + "]");
				cout << "\texpr: " << ("[" + b.first + "]") << "\n";
			}
		}

		// and it's time to build the query plan
		LogicalOpPtr tableScan = make_shared <LogicalTableScan> (tableRwPtr, 
		make_shared <MyDB_Table> (tableAlias + "_Table", tableAlias + "_StorageLoc", schema), 
		make_shared <MyDB_Stats> (tableRwPtr->getTable(), tableAlias), CNF, exprs);

		opPtrs.push_back(tableScan);
	}

	return opPtrs;
}

LogicalOpPtr SFWQuery :: buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {
	MyDB_SchemaPtr topSchema = make_shared <MyDB_Schema> ();
	LogicalOpPtr returnVal;

	// also, make sure that there are no aggregates in herre
	bool areAggs = false;
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString() << endl;
		if (a->hasAgg ()) {
			areAggs = true;
		}
	}

	vector <LogicalOpPtr> scanOpPtrs = getAllTableScan(allTableReaderWriters);

	// first, make sure we have exactly two tables... this prototype only works on two tables!!
	if (tablesToProcess.size () == 1) {
		returnVal = scanOpPtrs[0];

		if (groupingClauses.size () != 0 || areAggs) {
			topSchema = buildAggSchema(returnVal->getSchema ()->getAtts(), {tablesToProcess[0].second});

			returnVal  = make_shared <LogicalAggregate> (returnVal, 
			make_shared <MyDB_Table> ("aggTable", "aggStorageLoc", topSchema), valuesToSelect, 
			groupingClauses);
		}
	} else {
		// find the two input tables
		vector <pair <string, string>> joinTableNameAlias = {tablesToProcess[0], tablesToProcess[1]};
		vector <pair <string, MyDB_AttTypePtr>> joinSchemaAtts;

		if (groupingClauses.size () != 0 || areAggs) {
			
			LogicalOpPtr joinOp = joinTwoTable(joinSchemaAtts, scanOpPtrs, joinTableNameAlias, false);

			vector <string> tablesAlias = {tablesToProcess[0].second, tablesToProcess[1].second};

			MyDB_SchemaPtr joinSchema = buildAggSchema(joinSchemaAtts, tablesAlias);

			returnVal  = make_shared <LogicalAggregate> (joinOp, 
			make_shared <MyDB_Table> ("rtnTable", "rtnStorageLoc", joinSchema), valuesToSelect, 
			groupingClauses);
		} else {
			returnVal = joinTwoTable(joinSchemaAtts, scanOpPtrs, joinTableNameAlias, true);
		}
	}

	// done!!
	return returnVal;
}

LogicalOpPtr SFWQuery :: joinTwoTable (vector <pair <string, MyDB_AttTypePtr>> &atts, vector <LogicalOpPtr> scanOpPtrs,
										vector <pair <string, string>> tableNameAlias, bool finalJoin) {
	cout << "Start join two table" << endl;
	// find the various parts of the CNF
	vector <ExprTreePtr> leftCNF; 
	vector <ExprTreePtr> rightCNF; 
	vector <ExprTreePtr> topCNF; 
	MyDB_SchemaPtr topSchema = make_shared <MyDB_Schema> ();

	// loop through all of the disjunctions and break them apart
	for (auto a: allDisjunctions) {
		bool inLeft = a->referencesTable (tableNameAlias[0].second);
		bool inRight = a->referencesTable (tableNameAlias[1].second);
		if (inLeft && inRight) {
			cout << "top " << a->toString () << "\n";
			topCNF.push_back (a);
		}
	}

	MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
		
	// and see what we need from the left, and from the right
	for (auto b: scanOpPtrs[0]->getSchema()->getAtts ()) {
		totSchema->getAtts ().push_back (make_pair (b.first, b.second));
	}

	for (auto b: scanOpPtrs[1]->getSchema()->getAtts ()) {
		totSchema->getAtts ().push_back (make_pair (b.first, b.second));
	}

	// now we gotta figure out the top schema... get a record for the top
	if (finalJoin) {
		MyDB_Record myRec (totSchema);
		
		// and get all of the attributes for the output
		int i = 0;
		for (auto a: valuesToSelect) {
			topSchema->getAtts ().push_back (make_pair ("att_" + to_string (i++), myRec.getType (a->toString ())));
		}
	} else {
		topSchema = totSchema;
	}

	cout << "top schema: " << topSchema << "\n";
	atts = topSchema->getAtts();
	MyDB_TablePtr joinTablePtr = make_shared <MyDB_Table> ("topTable", "topStorageLoc", topSchema);

	if (finalJoin) {
		return make_shared <LogicalJoin> (scanOpPtrs[0], scanOpPtrs[1], 
		joinTablePtr, topCNF, valuesToSelect);
	} else {
		// All used attribute will be in the output if projection is empty
		vector <ExprTreePtr> emptyProjection;
		return make_shared <LogicalJoin> (scanOpPtrs[0], scanOpPtrs[1], 
		joinTablePtr, topCNF, emptyProjection);
	}
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
