#pragma once

typedef float    coord_t;
typedef float    dist_t;
typedef uint32_t idx_t;
typedef uint64_t label_t;

typedef struct HierarchicalNSW HierarchicalNSW;

bool hnsw_search(HierarchicalNSW* hnsw, const coord_t *point, size_t efSearch, size_t* n_results, label_t** results);
bool hnsw_add_point(HierarchicalNSW* hnsw, const coord_t *point, label_t label);
void hnsw_init(HierarchicalNSW* hnsw, size_t dim, size_t maxelements, size_t M, size_t maxM, size_t efConstruction);
int  hnsw_dimensions(HierarchicalNSW* hnsw);
size_t hnsw_count(HierarchicalNSW* hnsw);
size_t hnsw_sizeof(void);
