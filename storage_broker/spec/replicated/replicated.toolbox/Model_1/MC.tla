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
p1, p2
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
az1, az2, az3
----

\* MV CONSTANT definitions brokers
const_17373012151752000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_17373012151763000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_17373012151764000 == 
{p1, p2}
----

\* MV CONSTANT definitions azs
const_17373012151765000 == 
{az1, az2, az3}
----

\* SYMMETRY definition
symm_17373012151766000 == 
Permutations(const_17373012151752000) \union Permutations(const_17373012151763000) \union Permutations(const_17373012151764000) \union Permutations(const_17373012151765000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_17373012151767000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:5az_mapping
const_17373012151768000 == 
[ az1 |-> {b1,s1,p1} , az2 |-> {b2,s2,p2} , az3 |-> {b3,s3}]
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_17373012151769000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 16:40:15 CET 2025 by cs
