---- MODULE MC ----
EXTENDS replicated, TLC

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
p1
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
az1, az2, az3
----

\* MV CONSTANT definitions brokers
const_1737308178999123000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_1737308178999124000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_1737308178999125000 == 
{p1}
----

\* MV CONSTANT definitions azs
const_1737308178999126000 == 
{az1, az2, az3}
----

\* SYMMETRY definition
symm_1737308178999127000 == 
Permutations(const_1737308178999123000) \union Permutations(const_1737308178999124000) \union Permutations(const_1737308178999125000) \union Permutations(const_1737308178999126000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_1737308178999128000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:5az_mapping
const_1737308178999129000 == 
[ az1 |-> {b1,s1,p1} , az2 |-> {b2,s2} , az3 |-> {b3,s3}]
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_1737308178999130000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 18:36:19 CET 2025 by cs
