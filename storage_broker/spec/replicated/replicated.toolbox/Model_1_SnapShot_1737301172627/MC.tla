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
const_17373011685892000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_17373011685893000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_17373011685894000 == 
{p1, p2}
----

\* MV CONSTANT definitions azs
const_17373011685895000 == 
{az1, az2, az3}
----

\* SYMMETRY definition
symm_17373011685896000 == 
Permutations(const_17373011685892000) \union Permutations(const_17373011685893000) \union Permutations(const_17373011685894000) \union Permutations(const_17373011685895000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_17373011685897000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:5az_mapping
const_17373011685898000 == 
[ az1 |-> {b1,s1,p1} , az2 |-> {b2,s2,p2} , az3 |-> {b3,s3}]
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_17373011685899000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 16:39:28 CET 2025 by cs
