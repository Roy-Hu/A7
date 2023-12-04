
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

	// first, make sure we have exactly two tables... this prototype only works on two tables!!
	if (tablesToProcess.size () == 1) {
		vector <string> topExprs;
		vector <ExprTreePtr> topCNF; 

		MyDB_SchemaPtr scanSchema = make_shared <MyDB_Schema> ();

		string tableName = tablesToProcess[0].first;
		string tableAlias = tablesToProcess[0].second;

		MyDB_TablePtr tableIn = allTables[tableName];

		for (auto a: allDisjunctions) {
			topCNF.push_back (a);
		}
		
		for (auto b: tableIn->getSchema ()->getAtts ()) {
			bool needIt = false;
			for (auto a: valuesToSelect) {
				if (a->referencesAtt (tableAlias, b.first)) {
					needIt = true;
				}
			}

			if (needIt) {
				topSchema->getAtts ().push_back (make_pair (tableAlias + "_" + b.first, b.second));
				topExprs.push_back ("[" + b.first + "]");
			}
		}

		returnVal = make_shared <LogicalTableScan> (allTableReaderWriters[tableName], 
		make_shared <MyDB_Table> ("topTable", "topStorageLoc", topSchema), 
		make_shared <MyDB_Stats> (tableIn, tableAlias), topCNF, topExprs);

		if (groupingClauses.size () != 0 || areAggs) {
			topSchema = buildAggSchema(tableIn->getSchema ()->getAtts(), {tablesToProcess[0].second});

			returnVal  = make_shared <LogicalAggregate> (returnVal, 
			make_shared <MyDB_Table> ("aggTable", "aggStorageLoc", topSchema), valuesToSelect, 
			groupingClauses);
		}
	} else {
		// find the two input tables
		MyDB_TablePtr leftTable = allTables[tablesToProcess[0].first];
		MyDB_TablePtr rightTable = allTables[tablesToProcess[1].first];
		vector <pair <string, string>> joinTableNameAlias = {tablesToProcess[0], tablesToProcess[1]};
		vector <pair <string, MyDB_AttTypePtr>> joinSchemaAtts;

		if (groupingClauses.size () != 0 || areAggs) {

			LogicalOpPtr joinOp = joinTwoTable(leftTable, rightTable, joinSchemaAtts, allTableReaderWriters[tablesToProcess[0].first],
			allTableReaderWriters[tablesToProcess[1].first], joinTableNameAlias, false);

			vector <string> tablesAlias = {tablesToProcess[0].second, tablesToProcess[1].second};
			
			MyDB_SchemaPtr joinSchema = buildAggSchema(joinSchemaAtts, tablesAlias);

			returnVal  = make_shared <LogicalAggregate> (joinOp, 
			make_shared <MyDB_Table> ("rtnTable", "rtnStorageLoc", joinSchema), valuesToSelect, 
			groupingClauses);
		} else {
			returnVal = joinTwoTable(leftTable, rightTable, joinSchemaAtts, allTableReaderWriters[tablesToProcess[0].first], 
			allTableReaderWriters[tablesToProcess[1].first], joinTableNameAlias, true);
		}
	}

	// done!!
	return returnVal;
}

LogicalOpPtr SFWQuery :: joinTwoTable (MyDB_TablePtr leftTable, MyDB_TablePtr rightTable, vector <pair <string, MyDB_AttTypePtr>> &atts,
										MyDB_TableReaderWriterPtr lTableRWPtr, MyDB_TableReaderWriterPtr rTableRWPtr, 
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
		} else if (inLeft) {
			cout << "left: " << a->toString () << "\n";
			leftCNF.push_back (a);
		} else {
			cout << "right: " << a->toString () << "\n";
			rightCNF.push_back (a);
		}
	}

	// now get the left and right schemas for the two selections
	MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> ();
	MyDB_SchemaPtr rightSchema = make_shared <MyDB_Schema> ();
	MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
	vector <string> leftExprs;
	vector <string> rightExprs;
		
	// and see what we need from the left, and from the right
	for (auto b: leftTable->getSchema ()->getAtts ()) {
		bool needIt = false;
		for (auto a: valuesToSelect) {
			if (a->referencesAtt (tableNameAlias[0].second, b.first)) {
				needIt = true;
			}
		}
		for (auto a: topCNF) {
			if (a->referencesAtt (tableNameAlias[0].second, b.first)) {
				needIt = true;
			}
		}
		if (needIt) {
			leftSchema->getAtts ().push_back (make_pair (tableNameAlias[0].second + "_" + b.first, b.second));
			totSchema->getAtts ().push_back (make_pair (tableNameAlias[0].second + "_" + b.first, b.second));
			leftExprs.push_back ("[" + b.first + "]");
			cout << "left expr: " << ("[" + b.first + "]") << "\n";
		}
	}

	cout << "left schema: " << leftSchema << "\n";

	// and see what we need from the right, and from the right
	for (auto b: rightTable->getSchema ()->getAtts ()) {
		bool needIt = false;
		for (auto a: valuesToSelect) {
			if (a->referencesAtt (tableNameAlias[1].second, b.first)) {
				needIt = true;
			}
		}
		for (auto a: topCNF) {
			if (a->referencesAtt (tableNameAlias[1].second, b.first)) {
				needIt = true;
			}
		}
		if (needIt) {
			rightSchema->getAtts ().push_back (make_pair (tableNameAlias[1].second + "_" + b.first, b.second));
			totSchema->getAtts ().push_back (make_pair (tableNameAlias[1].second + "_" + b.first, b.second));
			rightExprs.push_back ("[" + b.first + "]");
			cout << "right expr: " << ("[" + b.first + "]") << "\n";
		}
	}
	cout << "right schema: " << rightSchema << "\n";
		
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

	// and it's time to build the query plan
	LogicalOpPtr leftTableScan = make_shared <LogicalTableScan> (lTableRWPtr, 
		make_shared <MyDB_Table> ("leftTable", "leftStorageLoc", leftSchema), 
		make_shared <MyDB_Stats> (leftTable, tableNameAlias[0].second), leftCNF, leftExprs);
	LogicalOpPtr rightTableScan = make_shared <LogicalTableScan> (rTableRWPtr, 
		make_shared <MyDB_Table> ("rightTable", "rightStorageLoc", rightSchema), 
		make_shared <MyDB_Stats> (rightTable, tableNameAlias[1].second), rightCNF, rightExprs);

	if (finalJoin) {
		return make_shared <LogicalJoin> (leftTableScan, rightTableScan, 
		joinTablePtr, topCNF, valuesToSelect);
	} else {
		// All used attribute will be in the output if projection is empty
		vector <ExprTreePtr> emptyProjection;
		return make_shared <LogicalJoin> (leftTableScan, rightTableScan, 
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
