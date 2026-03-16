#!/usr/bin/env python3
"""Query planner — cost-based optimizer for simple queries."""
import sys

class Table:
    def __init__(self,name,rows,indexes=None):
        self.name=name;self.rows=rows;self.indexes=indexes or set()

class Plan:
    def __init__(self,op,cost,children=None,detail=""):
        self.op=op;self.cost=cost;self.children=children or[];self.detail=detail
    def __repr__(self):return f"{self.op}(cost={self.cost:.0f}{', '+self.detail if self.detail else ''})"
    def tree(self,indent=0):
        s=" "*indent+repr(self)+"\n"
        for c in self.children:s+=c.tree(indent+2)
        return s

class Planner:
    def __init__(self):self.tables={}
    def add_table(self,name,rows,indexes=None):self.tables[name]=Table(name,rows,indexes)
    def plan_scan(self,table,predicate=None):
        t=self.tables[table]
        if predicate and predicate[0] in t.indexes:
            return Plan("IndexScan",t.rows*0.1,detail=f"{table}.{predicate[0]}")
        return Plan("SeqScan",t.rows,detail=table)
    def plan_join(self,left,right,on=None):
        nl_cost=left.cost*right.cost
        hj_cost=left.cost+right.cost*3
        if hj_cost<nl_cost:
            return Plan("HashJoin",hj_cost,[left,right],detail=str(on))
        return Plan("NestedLoop",nl_cost,[left,right],detail=str(on))
    def plan_query(self,tables,predicates=None,joins=None):
        scans=[self.plan_scan(t,predicates.get(t) if predicates else None) for t in tables]
        if len(scans)==1:return scans[0]
        result=scans[0]
        for s in scans[1:]:result=self.plan_join(result,s,joins)
        return result

def main():
    if len(sys.argv)>1 and sys.argv[1]=="--test":
        p=Planner()
        p.add_table("users",10000,{"id"})
        p.add_table("orders",100000,{"user_id"})
        # Sequential scan
        plan=p.plan_scan("users")
        assert plan.op=="SeqScan" and plan.cost==10000
        # Index scan
        plan2=p.plan_scan("users",("id","=","5"))
        assert plan2.op=="IndexScan" and plan2.cost==1000
        # Join
        s1=p.plan_scan("users",("id","=","5"))
        s2=p.plan_scan("orders")
        join=p.plan_join(s1,s2,"users.id=orders.user_id")
        assert join.op in("HashJoin","NestedLoop")
        # Full query plan
        qp=p.plan_query(["users","orders"],{"users":("id","=","5")})
        assert qp.cost>0
        print("All tests passed!")
    else:
        p=Planner();p.add_table("users",10000,{"id"});p.add_table("orders",100000,{"user_id"})
        plan=p.plan_query(["users","orders"],{"users":("id","=","5")})
        print(plan.tree())
if __name__=="__main__":main()
