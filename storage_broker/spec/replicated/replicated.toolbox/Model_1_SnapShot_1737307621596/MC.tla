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
const_173730761739711000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_173730761739712000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_173730761739713000 == 
{p1, p2}
----

\* MV CONSTANT definitions azs
const_173730761739714000 == 
{az1, az2, az3}
----

\* SYMMETRY definition
symm_173730761739715000 == 
Permutations(const_173730761739711000) \union Permutations(const_173730761739712000) \union Permutations(const_173730761739713000) \union Permutations(const_173730761739714000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_173730761739716000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:5az_mapping
const_173730761739717000 == 
[ az1 |-> {b1,s1,p1} , az2 |-> {b2,s2,p2} , az3 |-> {b3,s3}]
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_173730761739718000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 18:26:57 CET 2025 by cs
