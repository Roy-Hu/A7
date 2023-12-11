
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
// 
// note that this implementation only works for two-table queries that do not have an aggregation
// 

MyDB_SchemaPtr SFWQuery :: buildAggSchema (LogicalOpPtr joinOpPtr) {
	map <ExprTreePtr, bool> selectForGrouping;
	MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();
	
	vector <pair <string, MyDB_AttTypePtr>> joinSchemaAtts = joinOpPtr->getSchema()->getAtts();
	for (auto g : groupingClauses) {
		bool findAttr = false;
		for (auto a: valuesToSelect) {
			for (auto nameAlias : tablesToProcess) {
				string alias = nameAlias.second;

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

		bool find = false;

		for (auto b: joinSchemaAtts) {
			for (auto nameAlias : tablesToProcess) {
				if (a->referencesAtt (nameAlias.second, b.first)) {
					find = true;
					aggSchema->getAtts ().push_back (make_pair (nameAlias.second + "_" + b.first, b.second));
					break;
				}
			}

			if (find) break;
		}

		if (!find) {
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

pair <MyDB_JoinTreePtr, MyDB_StatsPtr> SFWQuery :: optimize (map <string, LogicalOpPtr> scanOpPtrs, map <vector <string>, pair <MyDB_JoinTreePtr, MyDB_StatsPtr>> &optimizedSet) {
	cout << "[OPTIMIZED] Start" << endl;
	double bestCost = numeric_limits<float>::infinity();
	pair <MyDB_JoinTreePtr, MyDB_StatsPtr> best = make_pair(make_shared <MyDB_JoinTree> (), make_shared <MyDB_Stats> ());	
    vector<pair<map <string, LogicalOpPtr>, map <string, LogicalOpPtr>>> subsetComplement;
	optimizedCnt++;
		
    // Loop through all possible subsets
    for (int i = 0; i < (1 << scanOpPtrs.size()); i++) {
        map <string, LogicalOpPtr> subset, complement;
        
        // Populate subset and complement based on the bitmask
		int j = 0;
        for (auto in : scanOpPtrs) {
            if (i & (1 << j)) {
				subset[in.first] = in.second;
            } else {
                complement[in.first] = in.second;
            }
			j++;
        }
        
        subsetComplement.emplace_back(subset, complement);
    }

	if (scanOpPtrs.size() == 1) {
		for (auto s : scanOpPtrs) {
			vector <string> curSet({s.first});
			if (optimizedSet.find(curSet) != optimizedSet.end()) {
				best = optimizedSet[curSet] = best;
			} else {
				best = make_pair(make_shared <MyDB_JoinTree> (s.first), s.second->cost().second);
				optimizedSet[curSet] = best;
			}
		}
	} else {
		for (auto sc : subsetComplement) {
			map <string, LogicalOpPtr> subset = sc.first;
			map <string, LogicalOpPtr> complements = sc.second; 
			
			if (subset.size() == 0 || complements.size() == 0) continue;

			vector <string> subsetName;
			vector <string> complementsName;
			vector <string> allNameLR;
			vector <string> allNameRL;
			vector <string> lTableAlias;
			vector <string> rTableAlias;

			for (auto s : subset) {
				subsetName.push_back(s.first);

				for (auto t : tablesToProcess) {
					if (t.first == s.first) {
						lTableAlias.push_back(t.second);
					}
				}
			}

			for (auto c : complements) {
				complementsName.push_back(c.first);

				for (auto t : tablesToProcess) {
					if (t.first == c.first) {
						lTableAlias.push_back(t.second);
					}
				}
			}

			allNameLR.reserve(subsetName.size() + complementsName.size());
			allNameLR.insert(allNameLR.end(), subsetName.begin(), subsetName.end());
			allNameLR.insert(allNameLR.end(), complementsName.begin(), complementsName.end());
			if (optimizedSet.find(allNameLR) != optimizedSet.end()) {
				return optimizedSet[allNameLR];
			}

			allNameRL.reserve(subsetName.size() + complementsName.size());
			allNameRL.insert(allNameRL.end(), subsetName.begin(), subsetName.end());
			allNameRL.insert(allNameRL.end(), complementsName.begin(), complementsName.end());
			if (optimizedSet.find(allNameRL) != optimizedSet.end()) {
				return optimizedSet[allNameRL];
			}	

			pair <MyDB_JoinTreePtr, MyDB_StatsPtr> lCost;
			pair <MyDB_JoinTreePtr, MyDB_StatsPtr> rCost;

			if (optimizedSet.find(subsetName) == optimizedSet.end()) {
				int i = 0;		
				printf("[OPTIMIZE] new set\n");
				for (auto s : subset) {
					cout << "\tTable " << i++ << ", "<< s.first <<endl;
				}
				lCost = optimize(subset, optimizedSet);
			} else {
				lCost = optimizedSet[subsetName];
			}

			if (optimizedSet.find(complementsName) == optimizedSet.end()) {
				printf("[OPTIMIZE] new set\n");
				int i = 0;
				for (auto s : complements) {
					cout << "\tTable " << i++ << ", "<< s.first <<endl;
				}
				rCost = optimize(complements, optimizedSet);
			} else {
				rCost = optimizedSet[complementsName];
			}

			MyDB_JoinTreePtr curJoinSeq = make_shared <MyDB_JoinTree> (lCost.first, rCost.first);

			vector <ExprTreePtr> CNF; 
			// loop through all of the disjunctions and break them apart
			for (auto a: allDisjunctions) {
				bool inLeft = false;
				bool inRight = false;

				for (auto b : lTableAlias) {
					if (a->referencesTable (b)) {
						inLeft = true;
						break;
					}
				}

				for (auto b : rTableAlias) {
					if (a->referencesTable (b)) {
						inRight = true;
						break;
					}
				}

				if (inLeft && inRight) {
					cout << "top " << a->toString () << "\n";
					CNF.push_back (a);
				}
			}
			
			MyDB_StatsPtr curStast = lCost.second->costJoin(CNF, rCost.second);
			if (curStast->getTupleCount() < bestCost) {
				best = make_pair(curJoinSeq, curStast);
				bestCost = curStast->getTupleCount();
			}

			optimizedSet[allNameLR] = make_pair(curJoinSeq, curStast);
			optimizedSet[allNameRL] = make_pair(curJoinSeq, curStast);
		}
	}

	return best;
}

map <string, LogicalOpPtr> SFWQuery :: getAllTableScan (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {
	map <string, LogicalOpPtr> opPtrs;
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

		opPtrs[tableName] = tableScan;
	}

	return opPtrs;
}

LogicalOpPtr SFWQuery :: buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {
	MyDB_SchemaPtr topSchema = make_shared <MyDB_Schema> ();
	LogicalOpPtr returnVal;
	bool isOptimize = false;

	// also, make sure that there are no aggregates in herre
	bool areAggs = false;
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString() << endl;
		if (a->hasAgg ()) {
			areAggs = true;
		}
	}

	map <string, LogicalOpPtr> scanOpPtrs = getAllTableScan(allTableReaderWriters);

	// first, make sure we have exactly two tables... this prototype only works on two tables!!
	if (tablesToProcess.size () == 1) {
		returnVal = scanOpPtrs[0];

		if (groupingClauses.size () != 0 || areAggs) {
			topSchema = buildAggSchema(returnVal);

			returnVal  = make_shared <LogicalAggregate> (returnVal, 
			make_shared <MyDB_Table> ("aggTable", "aggStorageLoc", topSchema), valuesToSelect, 
			groupingClauses);
		}
	} else if (isOptimize) {
		optimizedCnt = 0;
		map <vector <string>, pair <MyDB_JoinTreePtr, MyDB_StatsPtr>> optimizedSet;
		pair <MyDB_JoinTreePtr, MyDB_StatsPtr> opt = optimize(scanOpPtrs, optimizedSet);

		cout << "\n\nUsed " << optimizedCnt << " recurssions to find the OPT" << endl;
		cout << "OPT Cost " << opt.second->getTupleCount() << endl;
		cout << "Join table sequence" << endl;
		opt.first->print(opt.first);

		cout << "\n\n";
	} else {
		// find the two input tables
		pair <string, string> lhsNameAlias = tablesToProcess[0];
		pair <string, string> rhsNameAlias = tablesToProcess[1];
		vector <pair <string, string>> joinTableNameAlias = {lhsNameAlias, rhsNameAlias};

		LogicalOpPtr lhs = scanOpPtrs[lhsNameAlias.first], rhs;
		vector <pair <string, string>> lhsAlias = {lhsNameAlias};

		if (groupingClauses.size () != 0 || areAggs) {
			for (int i = 1; i < tablesToProcess.size(); i++) {
				rhsNameAlias = tablesToProcess[i];
				rhs = scanOpPtrs[rhsNameAlias.first];
				lhs = joinTwoTable(lhs, rhs, lhsAlias, {rhsNameAlias}, false);
				lhsAlias.push_back(rhsNameAlias);
			}

			MyDB_SchemaPtr joinSchema = buildAggSchema(lhs);

			returnVal  = make_shared <LogicalAggregate> (lhs, 
			make_shared <MyDB_Table> ("rtnTable", "rtnStorageLoc", joinSchema), valuesToSelect, 
			groupingClauses);
		} else {
			int i = 1;
			for (i = 1; i < tablesToProcess.size() - 1; i++) {
				rhsNameAlias = tablesToProcess[i];
				rhs = scanOpPtrs[rhsNameAlias.first];
				lhs = joinTwoTable(lhs, rhs, lhsAlias, {rhsNameAlias}, false);
				lhsAlias.push_back(rhsNameAlias);
			}

			rhsNameAlias = tablesToProcess[i];
			rhs = scanOpPtrs[rhsNameAlias.first];
			returnVal = joinTwoTable(lhs, rhs, lhsAlias, {rhsNameAlias}, true);
		}
	}

	// done!!
	return returnVal;
}

LogicalOpPtr SFWQuery :: joinTwoTable (LogicalOpPtr lOpPtr, LogicalOpPtr rOpPtr,
										vector <pair <string, string>> lTableNameAlias, vector <pair <string, string>> rTableNameAlias, bool finalJoin) {
	cout << "Start join two table" << endl;
	// find the various parts of the CNF
	vector <ExprTreePtr> leftCNF; 
	vector <ExprTreePtr> rightCNF; 
	vector <ExprTreePtr> topCNF; 
	MyDB_SchemaPtr topSchema = make_shared <MyDB_Schema> ();

	// loop through all of the disjunctions and break them apart
	for (auto a: allDisjunctions) {
		bool inLeft = false;
		bool inRight = false;

		for (auto b : lTableNameAlias) {
			if (a->referencesTable (b.second)) {
				inLeft = true;
				break;
			}
		}

		for (auto b : rTableNameAlias) {
			if (a->referencesTable (b.second)) {
				inRight = true;
				break;
			}
		}

		if (inLeft && inRight) {
			cout << "top " << a->toString () << "\n";
			topCNF.push_back (a);
		}
	}

	MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
		
	// and see what we need from the left, and from the right
	for (auto b: lOpPtr->getSchema()->getAtts ()) {
		totSchema->getAtts ().push_back (make_pair (b.first, b.second));
	}

	for (auto b: rOpPtr->getSchema()->getAtts ()) {
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
	MyDB_TablePtr joinTablePtr = make_shared <MyDB_Table> ("topTable", "topStorageLoc", topSchema);

	if (finalJoin) {
		return make_shared <LogicalJoin> (lOpPtr, rOpPtr, 
		joinTablePtr, topCNF, valuesToSelect);
	} else {
		// All used attribute will be in the output if projection is empty
		vector <ExprTreePtr> emptyProjection;
		return make_shared <LogicalJoin> (lOpPtr, rOpPtr, 
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
