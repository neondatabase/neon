---- MODULE MC ----
EXTENDS fullmesh, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
b1, b2, b3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
s1, s2, s3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
p1, p2
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
az1, az2, az3
----

\* MV CONSTANT definitions brokers
const_1737299350679194000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_1737299350679195000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_1737299350679196000 == 
{p1, p2}
----

\* MV CONSTANT definitions azs
const_1737299350679197000 == 
{az1, az2, az3}
----

\* SYMMETRY definition
symm_1737299350679198000 == 
Permutations(const_1737299350679194000) \union Permutations(const_1737299350679195000) \union Permutations(const_1737299350679196000) \union Permutations(const_1737299350679197000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_1737299350679199000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:5az_mapping
const_1737299350679200000 == 
[ az1 |-> {b1,s1,p1} , az2 |-> {b2,s2,p2} , az3 |-> {b3,s3}]
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_1737299350680201000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 16:09:10 CET 2025 by cs
