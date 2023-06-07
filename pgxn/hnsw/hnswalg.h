#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <cmath>
#include <queue>
#include <stdexcept>

extern "C" {
#include "hnsw.h"
}

struct HierarchicalNSW
{
	size_t maxelements;
	size_t cur_element_count;

	idx_t  enterpoint_node;

	size_t dim;
	size_t data_size;
	size_t offset_data;
	size_t offset_label;
	size_t size_data_per_element;
	size_t M;
	size_t maxM;
	size_t size_links_level0;
	size_t efConstruction;

#ifdef __x86_64__
	bool	use_avx2;
#endif

	char   data_level0_memory[0]; // varying size

  public:
	HierarchicalNSW(size_t dim, size_t maxelements, size_t M, size_t maxM, size_t efConstruction);
	~HierarchicalNSW();


	inline coord_t *getDataByInternalId(idx_t internal_id) const {
		return (coord_t *)&data_level0_memory[internal_id * size_data_per_element + offset_data];
	}

	inline idx_t *get_linklist0(idx_t internal_id) const {
		return (idx_t*)&data_level0_memory[internal_id * size_data_per_element];
	}

	inline label_t *getExternalLabel(idx_t internal_id) const {
		return (label_t *)&data_level0_memory[internal_id * size_data_per_element + offset_label];
	}

	std::priority_queue<std::pair<dist_t, idx_t>> searchBaseLayer(const coord_t *x, size_t ef);

	void getNeighborsByHeuristic(std::priority_queue<std::pair<dist_t, idx_t>> &topResults, size_t NN);

	void mutuallyConnectNewElement(const coord_t *x, idx_t id, std::priority_queue<std::pair<dist_t, idx_t>> topResults);

	void addPoint(const coord_t *point, label_t label);

	std::priority_queue<std::pair<dist_t, label_t>> searchKnn(const coord_t *query_data, size_t k);

	dist_t fstdistfunc(const coord_t *x, const coord_t *y);
};
