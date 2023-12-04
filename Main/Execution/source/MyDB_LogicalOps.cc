
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include <regex>

string removeAlias(string pred) {
    regex pattern(R"(\[\w+_(\w+_\w+)\])");

    pred = regex_replace(pred, pattern, "[$1]");

	return pred;
}

string concatPredicatesWithoutAlias(vector<ExprTreePtr> selectionPred) {
	if (selectionPred.size() == 0) {
		return "bool[true]";
	} else if (selectionPred.size() == 1) {
		return removeAlias(selectionPred[0]->toString());
	} else {
		string res = "&& (" + removeAlias(selectionPred[0]->toString()) + ", " + removeAlias(selectionPred[1]->toString()) + ")";

		for (int i = 2; i < selectionPred.size(); i++) {
			res = "&& (" + res + ", " + removeAlias(selectionPred[i]->toString()) + ")";
		}

		return res;
	}
}

string concatPredicates(vector<ExprTreePtr> selectionPred) {
	if (selectionPred.size() == 0) {
		return "bool[true]";
	} else if (selectionPred.size() == 1) {
		return selectionPred[0]->toString();
	} else {
		string res = "&& (" + selectionPred[0]->toString() + ", " + selectionPred[1]->toString() + ")";

		for (int i = 2; i < selectionPred.size(); i++) {
			res = "&& (" + res + ", " + selectionPred[i]->toString() + ")";
		}

		return res;
	}
}

// fill this out!  This should actually run the aggregation via an appropriate RelOp, and then it is going to
// have to unscramble the output attributes and compute exprsToCompute using an execution of the RegularSelection 
// operation (why?  Note that the aggregate always outputs all of the grouping atts followed by the agg atts.
// After, a selection is required to compute the final set of aggregate expressions)
//
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalAggregate :: execute () {
	MyDB_TableReaderWriterPtr tableIn = inputOp->execute();
	cout << endl << "[Aggregate]" << endl;
	MyDB_TableReaderWriterPtr tableOut = make_shared <MyDB_TableReaderWriter> (outputSpec, tableIn->getBufferMgr());
	vector <string> groupingStr;

	cout << "grouping" << endl;
	for (auto g : groupings) {
		groupingStr.push_back(g->toString());
		cout << "\t" << g->toString() << endl;
	}

	vector <pair <MyDB_AggType, string>> aggsToCompute;
	cout << "aggsToCompute" << endl;
	for (auto e : exprsToCompute) {
		if (e->isSum()) {
			aggsToCompute.push_back(make_pair(MyDB_AggType :: sum, e->toString().substr(4, e->toString().size() - 5)));
			cout << "\t" << e->toString().substr(4, e->toString().size() - 5) << endl;
		} else if (e->isAvg()) {
			aggsToCompute.push_back(make_pair(MyDB_AggType :: avg, e->toString().substr(4, e->toString().size() - 5)));
			cout << "\t"  << e->toString().substr(4, e->toString().size() - 5) << endl;
		} 
	}
	
	cout << "inSchema" << endl;
	cout << "\t" << tableIn->getTable()->getSchema() << endl;

	cout << "outSchema" << endl;
	cout << "\t" << outputSpec->getSchema() << endl;

	Aggregate myOp(tableIn, tableOut, aggsToCompute, groupingStr, "bool[true]");
	myOp.run();
	
	return tableOut;
}
// we don't really count the cost of the aggregate, so cost its subplan and return that
pair <double, MyDB_StatsPtr> LogicalAggregate :: cost () {
	return inputOp->cost ();
}
	
// this costs the entire query plan with the join at the top, returning the compute set of statistics for
// the output.  Note that it recursively costs the left and then the right, before using the statistics from
// the left and the right to cost the join itself
pair <double, MyDB_StatsPtr> LogicalJoin :: cost () {
	auto left = leftInputOp->cost ();
	auto right = rightInputOp->cost ();
	MyDB_StatsPtr outputStats = left.second->costJoin (outputSelectionPredicate, right.second);
	return make_pair (left.first + right.first + outputStats->getTupleCount (), outputStats);
}
	
// Fill this out!  This should recursively execute the left hand side, and then the right hand side, and then
// it should heuristically choose whether to do a scan join or a sort-merge join (if it chooses a scan join, it
// should use a heuristic to choose which input is to be hashed and which is to be scanned), and execute the join.
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)

// exprsToCompute -> valuesToSelect
// outputSelectionPredicate -> topCNF
MyDB_TableReaderWriterPtr LogicalJoin :: execute () {
	MyDB_TableReaderWriterPtr lTable = leftInputOp->execute();
	MyDB_TableReaderWriterPtr rTable = rightInputOp->execute();
	MyDB_TableReaderWriterPtr tableOut = make_shared <MyDB_TableReaderWriter> (outputSpec, lTable->getBufferMgr());
	vector <string> projections;
	vector <pair <string, string>> hashAtts;

	cout << "[JOIN]" << endl;

	cout << "projections" << endl;

	if (exprsToCompute.empty()) {
		for (auto o: lTable->getTable()->getSchema ()->getAtts ()) {
			projections.push_back   ("[" + o.first + "]");
			cout << "\t" << "[" + o.first + "]" << endl; 
		}
		for (auto o: rTable->getTable()->getSchema ()->getAtts ()) {
			projections.push_back   ("[" + o.first + "]");
			cout << "\t" << "[" + o.first + "]" << endl; 
		}
	} else {
		for (auto e : exprsToCompute) {
			projections.push_back(e->toString());
			cout << "\t" << e->toString() << endl;
		}
	}


	cout << "left Schema" << "\n\t" << lTable->getTable()->getSchema() << endl;  
	cout << "right Schema" << "\n\t" << rTable->getTable()->getSchema() << endl;  
	cout << "out Schema" << "\n\t" << outputSpec->getSchema() << endl;  

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	cout << "\nequality checks\n";
	for (auto o : outputSelectionPredicate) {
		if (o->isEq()) {
			lhs = o->getLHS();
			rhs = o->getRHS();

			if (lhs->isId() && rhs->isId()) {
				// Make sure the lsh matchs the left table
				bool lhsInLTable = false;
				for (auto a: lTable->getTable()->getSchema ()->getAtts ()) {
					if (lhs->getId() == a.first) {
						lhsInLTable = true;
						break;
					}
				}

				if (lhsInLTable) {
					hashAtts.push_back(make_pair(lhs->toString(), rhs->toString()));
					cout << "\t" << lhs->toString() << "\t" << rhs->toString() << endl;
				} else {
					hashAtts.push_back(make_pair(rhs->toString(), lhs->toString()));
					cout << "\t" << lhs->toString() << "\t" << rhs->toString() << endl;
				}
			}
		}
	}

	string finalSelectionPredicate = concatPredicates(outputSelectionPredicate);
	cout << "all predicates\n\t" << finalSelectionPredicate << endl;
	
	// SortMergeJoin myOp(lTable, rTable,
	// 	tableOut, finalSelectionPredicate, 
	// 	projections,
	// 	hashAtts[0], "bool[true]",
	// 	"bool[true]");

	ScanJoin myOp(lTable, rTable, tableOut, finalSelectionPredicate, projections, hashAtts, "bool[true]", "bool[true]");
	myOp.run ();

	cout << "Finish Join" << endl;
	return tableOut;
}

// this costs the table scan returning the compute set of statistics for the output
pair <double, MyDB_StatsPtr> LogicalTableScan :: cost () {
	MyDB_StatsPtr returnVal = inputStats->costSelection (selectionPred);
	return make_pair (returnVal->getTupleCount (), returnVal);	
}

// fill this out!  This should heuristically choose whether to use a B+-Tree (if appropriate) or just a regular
// table scan, and then execute the table scan using a relational selection.  Note that a desirable optimization
// is to somehow set things up so that if a B+-Tree is NOT used, that the table scan does not actually do anything,
// and the selection predicate is handled at the level of the parent (by filtering, for example, the data that is
// input into a join)
MyDB_TableReaderWriterPtr LogicalTableScan :: execute () {
	MyDB_TableReaderWriterPtr tableOut = make_shared <MyDB_TableReaderWriter> (outputSpec, inputSpec->getBufferMgr());
	cout << endl << "[Scan]" << endl;
	string allPred = concatPredicatesWithoutAlias(selectionPred);

	cout << "all predicates" << endl;
	cout << "\t" << allPred << endl;

	cout << "in schema" << endl;
	cout << "\t" << inputSpec->getTable()->getSchema() <<endl;

	cout << "out schema" << endl;
	cout << "\t" << outputSpec->getSchema() <<endl;

	cout << "projections" << endl;
	for (auto e : exprsToCompute) {
		cout << "\t" << e << endl;
	}

	RegularSelection myOp(inputSpec, tableOut, allPred, exprsToCompute);
	myOp.run();

	return tableOut;
}

#endif
