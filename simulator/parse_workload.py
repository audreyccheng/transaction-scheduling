from itertools import combinations, permutations
from scipy import stats
from scipy.stats import zipf
from scipy.cluster.vq import whiten
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.decomposition import PCA

import argparse
import collections
import copy
import json
import math
import numpy as np
import random
import sys
import time

# return powerset of given list, excluding empty set
def powerset(s):
    vals = []
    x = len(s)
    for i in range(1 << x):
        vals.append([s[j] for j in range(x) if (i & (1 << j))])
    return vals[1:]

class ComboWorkload:
    def __init__(self, workload_json, debug=False):
        self.cutoff = 1000
        self.workload = list(json.loads(workload_json).values())[:self.cutoff]
        print(len(self.workload))
        self.num_txns = len(self.workload)
        # self.powerset_workloads = powerset([i for i in range(self.num_txns)])
        self.debug = debug
        # print(self.powerset_workloads)

    def get_clusters(self, opt_cost, num_hot_keys):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        # workload.find_clusters(opt_cost)
        workload.find_clusters2(opt_cost, num_hot_keys)

    def get_fsj_cost(self):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        workload.get_fsj_cost()

    def get_greedy_cost(self, opt_cost):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        workload.get_greedy_cost(opt_cost)

    def get_greedy_opt_cost(self):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        workload.get_greedy_opt_cost()

    def get_greedy_cost_constrained(self, num_clusters, opt_cost):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        workload.get_greedy_cost_constrained(num_clusters, opt_cost)

    def get_greedy_cost_distr(self, num_samples, opt_cost):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        return workload.get_greedy_cost_distr(num_samples, opt_cost)

    def get_greedy_cost_sampled(self, num_samples, sample_rate, opt_cost):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        workload.get_greedy_cost_sampled(num_samples, sample_rate, opt_cost)

    def get_greedy_cost_distr_sampled(self, num_samples, num_txn_samples, sample_rate, opt_cost):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        workload.get_greedy_cost_distr_sampled(num_samples, num_txn_samples, sample_rate, opt_cost)

    def get_workload_seq(self, txn_seq, seq_num):
        txns = []
        for i in range(self.num_txns):
            txns.append(self.workload[i])

        workload = Workload(txns, self.debug, True)
        workload.get_seq_cost(txn_seq, seq_num)

    def get_workload(self, txn_seq):
        txns = []
        for i in txn_seq:
            txns.append(self.workload[i])

        workload = Workload(txns, self.debug, True)
        return workload.get_costs()

    def get_opt_workload(self, txn_seq):
        txns = []
        for i in txn_seq:
            txns.append(self.workload[i])

        workload = Workload(txns, self.debug, True)
        return workload.get_opt_costs()

    def get_random_workload(self, txn_seq, num_seqs, opt_cost):
        txns = self.workload

        workload = Workload(txns, self.debug, False)
        return workload.get_random_costs(num_seqs, opt_cost)

    def get_all_workloads(self): # TODO: find total tputs for all combos (e.g., [0], [1], [2,3])
        # for i in range(int(len(self.powerset_workloads[:-1])/2)):
        #     seq_1 = self.powerset_workloads[i]
        #     seq_2 = self.powerset_workloads[len(self.powerset_workloads)-i-2]
        #     cost_1 = self.get_workload(seq_1)
        #     cost_2 = self.get_workload(seq_2)
        #     print(seq_1, "cost: ", cost_1, seq_2, "cost: ", cost_2, " avg tput:", (len(seq_1) + len(seq_2)) / (cost_1 + cost_2))

        # all_txns = [x for x in range(self.num_txns)]
        # for i in range(2, self.num_txns):
        #     combos = list(combinations(all_txns, i))
        #     for j in range(len(combos)):
        #         print_str = ""
        #         total_seq = 0
        #         total_cost = 0
        #         combo = combos[j]
        #         seq = []
        #         for k in range(len(combo)):
        #             seq = [combo[k]]
        #             cost = self.get_workload(seq)
        #             print_str += "[" + ",".join(str(x) for x in seq) + "] cost: " + str(cost) + " "
        #             total_seq += len(seq)
        #             total_cost += cost
        #         other_txns = list(set(all_txns) - set(combo))
        #         cost = self.get_workload(other_txns)
        #         total_seq += len(other_txns)
        #         total_cost += cost
        #         print_str += "[" + ",".join(str(x) for x in other_txns) + "] cost: " + str(cost) + " avg tput: " + str(total_seq/total_cost)
        #         print(print_str)

        all_seq = [x for x in range(self.num_txns)] #self.powerset_workloads[-1]
        cost, min_len = self.get_workload(all_seq)
        print(all_seq, "cost: ", cost, "len: ", min_len, " tput:", len(all_seq) / min_len)

    def get_all_opt_workloads(self):
        all_seq = [x for x in range(self.num_txns)]
        min_len = self.get_opt_workload(all_seq)
        print(all_seq, "len: ", min_len, " tput:", len(all_seq) / min_len)

        # print("total num seqs: ", math.perm(len(all_seq), len(all_seq) - 1))

    def get_random_workloads(self, num_seqs, opt_cost):
        all_seq = [x for x in range(self.num_txns)] #self.powerset_workloads[-1]
        min_len = self.get_random_workload(all_seq, num_seqs, opt_cost)
        # print(all_seq, "len: ", min_len, " tput:", len(all_seq) / min_len)
        # print("all txns min cost: ", min_len)

    def get_rand_workload_seq(self, seq, opt_cost, print_val=False):
        workload = Workload(self.workload, self.debug, False)
        min_len = workload.get_random_seq_cost(seq, opt_cost, print_val)
        return min_len
        # print(seq, "len: ", min_len, " tput:", len(seq) / min_len)

    def get_random_sub_costs(self, num_breaks, num_seqs, opt_cost):
        num_groups = 1
        for i in range(num_breaks):
            txns = self.workload
            random.shuffle(txns)
            break_indicies = [200]#random.sample(range(self.num_txns), num_groups)
            break_indicies.sort()
            prev_index = 0
            min_costs = []
            for j in range(num_groups+1):
                break_index = self.num_txns
                if j != num_groups:
                    break_index = break_indicies[j]
                group = [txns[i] for i in range(prev_index,break_index)]
                workload = Workload(group, self.debug, False)
                min_len = workload.get_random_costs(num_seqs, opt_cost)
                min_costs.append(min_len)
                prev_index = break_index
            print("breaks: ", break_indicies, " min_costs: ", min_costs, " total: ", sum(min_costs))
            # first_half = [txns[i] for i in range(break_index)]
            # second_half = [txns[i] for i in range(break_index,self.num_txns)]
            # workload1 = Workload(first_half, self.debug, False)
            # min_len1 = workload1.get_random_costs(num_seqs, opt_cost)
            # workload2 = Workload(second_half, self.debug, False)
            # min_len2 = workload2.get_random_costs(num_seqs, opt_cost)
            # print("break: ", break_index, " min_cost1: ", min_len1, " min_cost2: ", min_len2, " total: ", min_len1+min_len2)


class Workload:
    def __init__(self, workload, debug=False, materialize=True, json_workload=None):
        self.cutoff = 100000
        self.workload = workload #json.loads(workload_json)
        if json_workload != None:
            self.workload = list(json.loads(json_workload).values())
        self.num_txns = len(self.workload) #.values()
        self.debug = debug
        self.txns = [] # list of txns (list of ops)
        self.sequences = [] # list of txn seqs (list of indices)
        self.cost_matrix = [] # indexed by time, prev_txn, curr_txn
        self.sequence_costs = [] # list of costs for each seq
        self.sequence_overlaps = [] # list of overlap for each seq
        self.sequence_overall = [] # list of costs account for overlap for each seq

        self.opt_cost_matrix = [] # indexed by time, prev_txn, curr_txn, no locking
        self.opt_sequence_costs = [] # list of costs for each seq, no locking

        self.get_txns()
        self.materialize = materialize
        if self.materialize:
            self.get_sequences()
            self.init_cost_matrix()

    # get transactions from json and represent hot keys as (r/w, key, position, txn_len)
    def get_txns(self):
        count = 0
        for txn in self.workload: #.values()
            txn_ops = []
            ops = txn.split(' ')
            txn_len = len(ops)
            for i in range(len(ops)):
                op = ops[i]
                if op != '*':
                    vals = op.split('-')
                    if len(vals) != 2:
                        print(vals, i, count)
                    assert len(vals) == 2
                    txn_ops.append((vals[0], vals[1], i+1, len(ops)))
            self.txns.append(txn_ops)
            count += 1
        if self.debug:
            print(self.txns)

    def find_clusters(self, opt_cost):
        pairwise_costs = np.zeros([self.num_txns, self.num_txns])
        total_costs = []
        start_time = time.time()
        for i in range(self.num_txns):
            # start_time = time.time()
            # i_list = []
            for j in range(self.num_txns):
                # print(i,j)
                # if i == j:
                #     continue
                if opt_cost:
                    pairwise_costs[i][j] = self.get_opt_seq_cost([i,j], 0) #- len(self.txns[i])
                else:
                    # i_list.append(self.get_seq_cost([i,j], 0))
                    pairwise_costs[i][j] = self.get_seq_cost([i,j], 0) #max(self.get_seq_cost([i,j], 0), self.get_seq_cost([j,i], 0))
                # x = list(pairwise_costs[i])
                # x.sort()
                # pairwise_costs[i] = x
            total_costs.append(sum(pairwise_costs[i]))
            # pairwise_costs.append(i_list)
            # print("time: ", time.time() - start_time)
        print("time: ", time.time() - start_time)
        print(pairwise_costs)
        print(pairwise_costs.shape)
        # whiten(pairwise_costs)
        # print(pairwise_costs)
        # print(total_costs)

        # thres_costs = np.zeros([self.num_txns, self.num_txns])
        # avg_cost = sum(pairwise_costs[i]) / self.num_txns
        # for i in range(self.num_txns):
        #     for j in range(self.num_txns):
        #         if pairwise_costs[i][j] < avg_cost:
        #             thres_costs[i][j] = 0
        #         else:
        #             thres_costs[i][j] = 1
        # print(avg_cost)
        # print(thres_costs)

        # hierarchical_cluster = AgglomerativeClustering(n_clusters=6, metric='euclidean', linkage='ward') #
        # labels = hierarchical_cluster.fit_predict(pairwise_costs)
        # print(labels)

        kmeans = KMeans(n_clusters=2, random_state=0, n_init='auto').fit(pairwise_costs)
        labels = kmeans.labels_
        print(kmeans.labels_)
        # print(kmeans.cluster_centers_.shape)
        # print(kmeans.get_feature_names_out())

        pca = PCA(n_components=1)
        transformed = pca.fit_transform(pairwise_costs)
        # print(transformed)

        val_map = {}
        label_map = {}

        for i in range(len(kmeans.labels_)):
            # print(self.workload[i], kmeans.labels_[i], transformed[i][0])
            rounded = int(transformed[i][0] * 10)
            if rounded not in val_map:
                val_map[rounded] = [self.workload[i]]
            # else:
            #     val_map[rounded].append(self.workload[i])
            if kmeans.labels_[i] not in label_map:
                label_map[kmeans.labels_[i]] = [self.workload[i]]
            else:
                if self.workload[i] not in label_map[kmeans.labels_[i]]:
                    label_map[kmeans.labels_[i]].append(self.workload[i])

        # for k,v in zip(label_map.keys(),label_map.values()):
        #     print(k,v)

        # # print(val_map)
        # # print(len(val_map.values()))
        # od = collections.OrderedDict(sorted(val_map.items()))
        # print(len(od.keys()))
        # print(od.keys())
        # print(od.values())

        thres_costs = [[-5,0,1,0],[0,-5,1,0],[-5,0,0,1],[0,-5,0,1],
                       [1,0,5,0],[0,1,5,0],[1,0,0,5],[0,1,0,5]]

        thres_costs = [[-5,0,1,0,0,0],
                       [-5,0,0,1,0,0],
                       [-5,0,0,0,0,1],
                       [0,-5,1,0,0,0],[0,-5,0,1,0,0],[0,-5,0,0,0,1],
                       [0,0,1,0,-5,0],[0,0,0,1,-5,0],[0,0,0,0,-5,1],
                       [1,0,5,0,0,0],
                       [1,0,0,5,0,0],
                       [1,0,0,0,0,5],
                       [0,1,5,0,0,0],[0,1,0,5,0,0],[0,1,0,0,0,5],
                       [0,0,5,0,1,0],[0,0,0,5,1,0],[0,0,0,0,1,5],]

        # thres_costs = [[17,0,5,0,0,0],
        #                [17,0,0,5,0,0],
        #                [0,17,5,0,0,0],
        #                [0,17,0,5,0,0],
        #                [0,0,17,0,5,0],
        #                [0,0,17,0,0,5],
        #                [0,0,0,17,5,0],
        #                [0,0,0,17,0,5],
        #                [5,0,0,0,17,0],
        #                [0,5,0,0,17,0],
        #                [5,0,0,0,0,17],
        #                [0,5,0,0,0,17]]

        # thres_costs = [[1,1,0,0,5,5,5,0],
        #                [1,1,0,0,5,5,0,5],
        #                [0,0,1,1,5,0,5,5],
        #                [0,0,1,1,0,5,5,5],
        #                [5,5,5,0,1,1,0,0],
        #                [5,5,0,5,1,1,0,0],
        #                [5,0,5,5,0,0,1,1],
        #                [0,5,5,5,0,0,1,1]]

        # thres_costs = [[1,1,0,0,5,5,5,0],
        #                [1,1,0,0,5,5,0,5],
        #                [0,0,1,1,5,0,5,5],
        #                [0,0,1,1,0,5,5,5],
        #                [5,5,5,0,5,5,5,0],
        #                [5,5,0,5,5,5,0,5],
        #                [5,0,5,5,5,0,5,5],
        #                [0,5,5,5,0,5,5,5]]

        kmeans = KMeans(n_clusters=3, random_state=0, n_init='auto').fit(thres_costs)
        print(kmeans.labels_)

        pca = PCA(n_components=1)
        transformed = pca.fit_transform(thres_costs)
        # print(transformed)

        # kmeans = KMeans(n_clusters=3, random_state=0, n_init='auto').fit(transformed)
        # print(kmeans.labels_)

        return labels

    def find_types(self, labels):
        in_file = open('types.txt', 'r')
        types_str = ""
        for line in in_file:
            types_str = line
            break
        types = types_str.split(",")[:self.cutoff]
        # print(types)

        type_map = {} # <type, labels>
        for l, t in zip(labels, types):
            if t not in type_map:
                type_map[t] = [l]
            else:
                if l not in type_map[t]:
                    type_map[t].append(l)
        print(type_map)

    def find_clusters2(self, opt_cost, num_hot_keys):
        print("finding clusters")
        start_time = time.time()
        costs = np.zeros([self.num_txns, num_hot_keys])
        hot_key_index = []
        for i in range(self.num_txns):
            txn = self.txns[i]
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                if int(key) > num_hot_keys - 1:
                    continue
                if key not in hot_key_index:
                    hot_key_index.append(key)
                key_index = hot_key_index.index(key)
                val = txn_len - pos + 1
                # if op_type == 'r':
                #     val *= -1
                if val > costs[i][key_index]:
                    costs[i][key_index] = val
            if i % 10000 == 0:
                print("time: ", time.time() - start_time)
                # print(txn)
        print("time: ", time.time() - start_time)
        print(costs[0])

        cost_map = {}
        for i in costs:
            x = tuple(i)
            if x not in cost_map:
                cost_map[x] = 1
            else:
                cost_map[x] += 1
        # print(cost_map)
        print(len(cost_map.keys()))
        print(len(costs))

        start_time = time.time()
        kmeans = KMeans(n_clusters=num_hot_keys, random_state=0, n_init='auto').fit(costs)
        print("kmeans time: ", time.time() - start_time)
        labels = kmeans.labels_
        print(kmeans.labels_)
        print(kmeans.inertia_)
        self.find_types(kmeans.labels_)
        print("time: ", time.time() - start_time)

        Sum_of_squared_distances = []
        K = list(range(10,1000,100))
        for num_clusters in K :
            kmeans = KMeans(n_clusters=num_clusters, n_init='auto')
            kmeans.fit(costs)
            Sum_of_squared_distances.append(kmeans.inertia_)
            self.find_types(kmeans.labels_)
        print(K)
        print(Sum_of_squared_distances)
        print("time: ", time.time() - start_time)

        # thres_costs = [[2,1,3,0,0,0,0,0,0,0],
        #                [0,0,3,2,1,0,0,0,0,0],
        #                [3,0,4,2,1,0,0,0,0,0],
        #                [0,0,0,0,0,2,3,1,0,0],
        #                [0,0,0,0,0,0,3,2,0,1],
        #                [0,0,0,0,0,0,3,0,2,1],
        #                [0,0,0,3,0,0,4,2,1,0],
        #             #    [2,1,3,0,0,0,0,0,0,0],
        #             #    [0,0,3,2,1,0,0,0,0,0],
        #             #    [3,0,4,2,1,0,0,0,0,0],
        #             #    [0,0,0,0,0,2,3,1,0,0],
        #             #    [0,0,0,0,0,0,3,2,0,1],
        #             #    [0,0,0,0,0,0,3,0,2,1],
        #             #    [0,0,0,3,0,0,4,2,1,0],
        #                ]

        thres_costs = [[15,10,0,0],
                       [15,0,10,0],
                       [15,10,0,0],
                       [15,1,0,0],
                       [15,0,1,0],
                       [15,1,0,0],]

        kmeans = KMeans(n_clusters=3, random_state=0, n_init='auto').fit(thres_costs)
        labels = kmeans.labels_
        print(kmeans.labels_)

    # get all possible sequences of transactions
    def get_sequences(self):
        self.sequences = list(permutations([i for i in range(len(self.txns))]))

    def init_cost_matrix(self):
        self.cost_matrix = np.zeros([len(self.txns), len(self.txns), len(self.txns)])
        self.sequence_costs = np.zeros([len(self.sequences)])
        self.sequence_overlaps = np.zeros([len(self.sequences)])

        self.opt_cost_matrix = np.zeros([len(self.txns), len(self.txns), len(self.txns)])
        self.opt_sequence_costs = np.zeros([len(self.sequences)])

    def get_incremental_seq_cost(self, t_index, key_map, total_cost):
        txn = self.txns[t_index]

        txn_start = 1
        txn_total_len = 0
        max_release = 0
        cost = 0
        for j in range(len(txn)):
            (op_type, key, pos, txn_len) = txn[j]
            if key in key_map:
                key_start = 0
                if key_map[key][-1][0] == 'w' or op_type == 'w':
                    key_start = key_map[key][-1][2] + 1 # get end time of latest lock end
                else:
                    key_start = key_map[key][-1][1] #pos # read locks shared ... but need to start at earliest as before
                txn_start = max(txn_start, key_start - pos + 1) # place txn start behind conflicting locks
                if self.debug:
                    print(key, key_start, pos, txn_start, key_map[key][-1][0] == 'w', op_type == 'w', key_map[key])
                max_release = max(max_release, key_start - 1) # latest release of all locks
            txn_total_len = txn_len
        txn_end = txn_start + txn_total_len - 1
        cost = txn_end - total_cost

        if txn_end <= total_cost: # in some cases, later txn in seq can finish first
            cost = 0

        total_cost += cost
        if self.debug:
            print(txn, txn_start, txn_end, max_release, cost, total_cost)
        if self.debug:
            print(txn_start, txn_end, max_release, cost)

        for j in range(len(txn)):
            (op_type, key, pos, txn_len) = txn[j]
            key_start = txn_start + pos - 1
            if key in key_map:
                if key_map[key][-1][0] == 'w' or op_type == 'w':
                    key_map[key].append((op_type, key_start, txn_end))
                else: # make sure read lock is held for appropriate duration
                    key_map[key].append((op_type, key_start, max(txn_end, key_map[key][-1][2])))
            else:
                key_map[key] = [(op_type, key_start, txn_end)]
        if self.debug:
            print(total_cost)

        if self.debug:
            print("seq: ", t_index, key_map, total_cost)

        return key_map, total_cost

    def get_seq_cost(self, txn_seq, seq_num):
        if self.debug:
            print("seq: ", txn_seq, seq_num)
        key_map = {} # <key, [(r/w, lock_start, lock_end)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        for i in range(len(txn_seq)):
            time = i
            txn = self.txns[txn_seq[i]]
            if self.debug:
                print(txn)
            txn_start = 1
            txn_total_len = 0
            max_release = 0
            cost = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                # if int(key) < 1000:
                #     continue
                if key in key_map:
                    key_start = 0
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        key_start = key_map[key][-1][2] + 1 # get end time of latest lock end
                    else:
                        key_start = key_map[key][-1][1] #pos # read locks shared ... but need to start at earliest as before
                    txn_start = max(txn_start, key_start - pos + 1) # place txn start behind conflicting locks
                    if self.debug:
                        print(key, key_start, pos, txn_start, key_map[key][-1][0] == 'w', op_type == 'w', key_map[key])
                    max_release = max(max_release, key_start - 1) # latest release of all locks
                txn_total_len = txn_len
            txn_end = txn_start + txn_total_len - 1
            cost = txn_end - total_cost #max_release
            # if max_release == 0:
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            # else:
            #     cost = txn_end - total_cost
            total_cost += cost
            if self.debug:
                print(txn, txn_start, txn_end, max_release, cost, total_cost)

            curr_txn = txn_seq[i]
            if self.materialize:
               self.cost_matrix[time][prev_txn, curr_txn] = cost
            prev_txn = curr_txn
            if self.debug:
                print(txn_start, txn_end, max_release, cost)

            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = txn_start + pos - 1
                if key in key_map:
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        key_map[key].append((op_type, key_start, txn_end))
                    else: # make sure read lock is held for appropriate duration
                        key_map[key].append((op_type, key_start, max(txn_end, key_map[key][-1][2])))
                else:
                    key_map[key] = [(op_type, key_start, txn_end)]
            if self.debug:
                print(key_map)
        if self.debug:
            print(total_cost)
        if self.materialize:
            self.sequence_costs[seq_num] = total_cost

        # overlap = total_cost
        # visited_keys = {} # <key, count in seq>
        # for i in range(len(txn_seq)):
        #     txn = self.txns[txn_seq[i]]
        #     for j in range(len(txn)):
        #         # iterate through keys in order to find overlap
        #         (_, key, pos, txn_len) = txn[j]
        #         if key not in visited_keys:
        #             visited_keys[key] = 0
        #         key_index = visited_keys[key]
        #         assert key in key_map and len(key_map[key]) > key_index
        #         (op_type, key_lock_start, key_lock_end) = key_map[key][key_index]
        #         (op_type2, _, key_end) = key_map[key][-1] # final key release
        #         if self.debug:
        #             print(txn, key, key_end, key_lock_start, overlap, op_type, op_type2)
        #         if key_end == total_cost and (op_type == 'w' or op_type2 == 'w'):
        #             overlap = min(overlap, key_lock_start - 1)
        #             break
        #         else:
        #             if op_type == 'w' or op_type2 == 'w': # no bounds on read overlap
        #                 overlap = min(overlap, key_lock_start - 1)
        #         visited_keys[key] += 1

        # if self.materialize:
        #     self.sequence_overlaps[seq_num] = overlap
        # if self.debug:
        #     print(total_cost, overlap)

        return total_cost

    def get_rc_seq_cost(self, txn_seq, seq_num):
        if self.debug:
            print("seq: ", txn_seq, seq_num)
        key_map = {} # <key, [(r/w, lock_start, lock_end)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        for i in range(len(txn_seq)):
            time = i
            txn = self.txns[txn_seq[i]]
            if self.debug:
                print(txn)
            txn_start = 1
            txn_total_len = 0
            max_release = 0
            cost = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                # if int(key) < 1000:
                #     continue
                if key in key_map:
                    key_start = 0
                    if key_map[key][-1][0] == 'w':
                        key_start = key_map[key][-1][2] + 1 # get end time of latest lock end
                    elif op_type == 'w':
                        key_start = key_map[key][-1][1] + 1 # get end time of read lock TODO: check this
                    else:
                        key_start = key_map[key][-1][1] #pos # read locks shared ... but need to start at earliest as before
                    txn_start = max(txn_start, key_start - pos + 1) # place txn start behind conflicting locks
                    if self.debug:
                        print(key, key_start, pos, txn_start, key_map[key][-1][0] == 'w', op_type == 'w', key_map[key])
                    max_release = max(max_release, key_start - 1) # latest release of all locks
                txn_total_len = txn_len
            txn_end = txn_start + txn_total_len - 1
            cost = txn_end - total_cost #max_release
            # if max_release == 0:
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            # else:
            #     cost = txn_end - total_cost
            total_cost += cost
            if self.debug:
                print(txn, txn_start, txn_end, max_release, cost, total_cost)

            curr_txn = txn_seq[i]
            if self.materialize:
               self.cost_matrix[time][prev_txn, curr_txn] = cost
            prev_txn = curr_txn
            if self.debug:
                print(txn_start, txn_end, max_release, cost)

            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = txn_start + pos - 1
                if key in key_map:
                    if key_map[key][-1][0] == 'w' or op_type == 'w': # TODO: check this
                        key_map[key].append((op_type, key_start, txn_end))
                    else: # make sure read lock is held for appropriate duration
                        key_map[key].append((op_type, key_start, key_map[key][-1][1])) # TODO: check this
                else:
                    key_map[key] = [(op_type, key_start, txn_end)]
            if self.debug:
                print(key_map)
        if self.debug:
            print(total_cost)

        return total_cost

    def find_earliest_read(self, key, key_map, txn_id):
        # must use latest read if part of same txn
        if key_map[key][-1][3] == txn_id:
            return key_map[key][-1][1]
        else:
            if self.debug:
                print(key, key_map[key], txn_id)
            index = len(key_map[key]) - 1
            while key_map[key][index][0] == 'r':
                if index == 0:
                    break
                index -= 1
            return index

    def get_incremental_opt_seq_cost(self):
        pass

    def get_opt_seq_cost(self, txn_seq, seq_num):
        if self.debug:
            print("seq: ", txn_seq, seq_num)
        key_map = {} # <key, [(r/w, lock_start, lock_end, txn_id)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        txn_id = 0
        for i in range(len(txn_seq)):
            time = i
            txn = self.txns[txn_seq[i]]
            txn_start = 1
            txn_total_len = 0
            max_release = 0
            cost = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                if key in key_map:
                    key_start = 0
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        key_start = key_map[key][-1][2] + 1 # get end time of latest lock end
                    else:
                        key_start = self.find_earliest_read(key, key_map, txn_id)
                        # key_start = key_map[key][-1][1] #pos # read locks shared
                    txn_start = max(txn_start, key_start - pos + 1) # place txn start behind conflicting locks
                    if self.debug:
                        print(key, key_start, pos, txn_start)
                    max_release = max(max_release, key_start - 1) # latest release of all locks
                txn_total_len = txn_len
            txn_end = txn_start + txn_total_len - 1
            cost = txn_end - total_cost #max_release
            # if max_release == 0:
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            # else:
            #     cost = txn_end - total_cost
            total_cost += cost
            if self.debug:
                print(txn, txn_start, txn_end, max_release, cost, total_cost)

            curr_txn = txn_seq[i]
            if self.materialize:
                self.opt_cost_matrix[time][prev_txn, curr_txn] = cost
            prev_txn = curr_txn
            if self.debug:
                print(txn_start, txn_end, max_release, cost)

            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = txn_start + pos - 1
                if key in key_map:
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        key_map[key].append((op_type, key_start, key_start, txn_id))
                    else:
                        key_map[key].append((op_type, key_start, key_start, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_start, txn_id)]
            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            print(total_cost)
        if self.materialize:
            self.opt_sequence_costs[seq_num] = total_cost

        return total_cost

    # schmoy's alg: assuming only writes and no serializability
    def get_fsj_cost(self):
        # get max len, as defined in paper
        max_len = 0
        max_len_map = {}
        for i in range(self.num_txns):
            txn = self.txns[i]
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                if not key in max_len_map:
                    max_len_map[key] = [pos]
                else:
                    max_len_map[key].append(pos)
        if self.debug:
            print(max_len_map)

        for k, indices in max_len_map.items():
            i_map = {}
            for index in indices:
                if not index in i_map:
                    i_map[index] = 1
                else:
                    i_map[index] += 1
            for index, occurrences in i_map.items():
                max_len = max(max_len, index + occurrences - 1)
        if self.debug:
            print(max_len)

        # get perturbations
        delays = []
        for i in range(self.num_txns):
            delays.append(random.randint(0,max_len))
        if self.debug:
            print("delays: ", delays)

        # get makespan of schedule, as defined in paper
        time_map = {} # <time, [keys]>
        txn_seq_map = {} # <time, [txn_ids]>
        for i in range(self.num_txns):
            delay = delays[i]
            txn = self.txns[i]
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                time = pos + delay
                if time not in time_map:
                    time_map[time] = [key]
                else:
                    time_map[time].append(key)
                if j == 0:
                    if time not in txn_seq_map:
                        txn_seq_map[time] = [i]
                    else:
                        txn_seq_map[time].append(i)

        od = collections.OrderedDict(sorted(time_map.items()))
        odt = collections.OrderedDict(sorted(txn_seq_map.items()))
        if self.debug:
            print(time_map)
            print(txn_seq_map)
            print(od)
            print(odt)

        # get makespan with opt cost and no serializability
        # (possible that delay causes longer makespan)
        txn_seq = []
        for index, ids in odt.items():
            txn_seq.extend(ids)
        key_map = {} # <key, [(r/w, lock_start, lock_end, txn_id)]>
        max_key_index = 0
        for i in range(len(txn_seq)):
            time = i
            txn = self.txns[txn_seq[i]]
            txn_start = 1
            txn_total_len = 0
            max_release = 0
            cost = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                if not key in key_map:
                    key_map[key] = [(op_type, pos, pos, i)]
                    max_key_index = max(max_key_index, pos)
                else:
                    key_start = max(key_map[key][-1][2] + 1, pos) # FIX: we need to find key_start first or can violate precedence constraints
                    key_map[key].append((op_type, key_start, key_start, i))
                    max_key_index = max(max_key_index, key_start)
        if self.debug:
            print(txn_seq)
            print(key_map)
        print("max key index (BROKEN): ", max_key_index)

        # get opt cost assuming serializability
        opt_cost = self.get_opt_seq_cost(txn_seq, 0)
        print("opt cost of seq: ", opt_cost)

        new_time_map = {} # <time, [keys]>
        for time, keys in od.items():
            all_keys = keys
            if time in new_time_map:
                all_keys.extend(new_time_map[time])
            seen = set()
            uniq = [x for x in all_keys if x not in seen and not seen.add(x)]
            seen = set()
            dupes = [x for x in all_keys if x in seen or seen.add(x)]

            if self.debug:
                print(time, all_keys, uniq, dupes)

            new_time_map[time] = uniq
            while len(dupes) > 0:
                k = dupes[0]
                new_time = time + 1
                while new_time in new_time_map and k in new_time_map[new_time]:
                    new_time += 1
                if not new_time in new_time_map:
                    new_time_map[new_time] = [k]
                else:
                    new_time_map[new_time].append(k)
                if self.debug:
                    print(new_time)
                dupes.pop(0)

        odn = collections.OrderedDict(sorted(new_time_map.items()))
        if self.debug:
            print(odn)
        print("fsj cost:", max(odn.keys()))
        print("lower bound: ", int(math.log(math.log(self.num_txns)) / math.log(self.num_txns) * math.log(self.num_txns) * max(odn.keys())))

    def get_greedy_cost_all(self):
        # sort txns by lock hold duration
        txn_map = {} # <total lock hold duration, [ids]>
        for i in range(self.num_txns):
            txn = self.txns[i]
            txn_score = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                txn_score += txn_len - pos + 1
            if not txn_score in txn_map:
                txn_map[txn_score] = [i]
            else:
                txn_map[txn_score].append(i)
        od = collections.OrderedDict(sorted(txn_map.items(), reverse=True))
        if self.debug:
            print(od)

        # starting with txn with greatest lock hold times,
        # greedily add transactions to schedule based on which increases makespan the least
        start_txn = list(od.items())[0][1][0]
        txn_seq = [start_txn]
        for i in range(self.num_txns - 1):
            min_cost = 100000 # MAX
            min_txn = -1
            for j in range(self.num_txns):
                if j in txn_seq:
                    continue
                test_seq = txn_seq.copy()
                test_seq.append(j)
                cost = self.get_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    min_txn = j
            txn_seq.append(min_txn)
        if self.debug:
            print(txn_seq)

        overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost (sorted): ", overall_cost)

        # greedy with random starting point
        start_txn = random.randint(0, self.num_txns - 1)
        txn_seq = [start_txn]
        for i in range(self.num_txns - 1):
            min_cost = 100000 # MAX
            min_txn = -1
            for j in range(self.num_txns):
                if j in txn_seq:
                    continue
                test_seq = txn_seq.copy()
                test_seq.append(j)
                cost = self.get_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    min_txn = j
            txn_seq.append(min_txn)
        if self.debug:
            print(txn_seq)

        overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost: ", overall_cost)

        # greedy going down sorted list
        txn_seq = []
        for _, txns in od.items():
            for t in txns:
                if len(txn_seq) == 0:
                    txn_seq.append(t)
                    continue
                min_cost = 100000 # MAX
                insert_index = -1
                for i in range(len(txn_seq)):
                    test_seq = txn_seq.copy()
                    test_seq.insert(i, t)
                    cost = self.get_seq_cost(test_seq, 0)
                    if cost < min_cost:
                        min_cost = cost
                        insert_index = i
                txn_seq.insert(insert_index, t)
        if self.debug:
            print(txn_seq)

        overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost (insert, sorted): ", overall_cost)

        # greedy in random order
        txn_seq = []
        indicies = random.sample(range(self.num_txns), self.num_txns)
        for i in range(self.num_txns):
            t = indicies.pop(0)
            if len(txn_seq) == 0:
                txn_seq.append(t)
                continue
            min_cost = 100000 # MAX
            insert_index = -1
            for i in range(len(txn_seq)):
                test_seq = txn_seq.copy()
                test_seq.insert(i, t)
                cost = self.get_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    insert_index = i
            txn_seq.insert(insert_index, t)
        if self.debug:
            print(txn_seq)

        overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost (insert): ", overall_cost)

    def get_greedy_opt_cost(self):
        # greedy with random starting point
        start_txn = random.randint(0, self.num_txns - 1)
        txn_seq = [start_txn]
        for i in range(self.num_txns - 1):
            min_cost = 100000 # MAX
            min_txn = -1
            for j in range(self.num_txns):
                if j in txn_seq:
                    continue
                test_seq = txn_seq.copy()
                test_seq.append(j)
                cost = self.get_opt_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    min_txn = j
            txn_seq.append(min_txn)
        if self.debug:
            print(txn_seq)

        overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        print("greedy cost: ", overall_cost)

    def get_greedy_cost_range(self, start, end, opt_cost):
        # greedy with random starting point
        start_txn = random.randint(start, end - 1)
        txn_seq = [start_txn]
        for i in range(start, end - 1):
            min_cost = 100000 # MAX
            min_txn = -1
            for j in range(start, end):
                if j in txn_seq:
                    continue
                test_seq = txn_seq.copy()
                test_seq.append(j)
                cost = 0
                if opt_cost:
                    cost = self.get_opt_seq_cost(test_seq, 0)
                else:
                    cost = self.get_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    min_txn = j
            txn_seq.append(min_txn)
        if self.debug:
            print(txn_seq)

        return txn_seq

    def get_greedy_cost_constrained(self, num_clusters, opt_cost):
        txn_seq = []
        start = 0
        end = int(self.num_txns / num_clusters)
        for i in range(num_clusters):
            txn_seq.extend(self.get_greedy_cost_range(start, end, opt_cost))
            if self.debug:
                print(start, end, txn_seq)
            start = end
            end = int(self.num_txns * (i + 2) / num_clusters)
        if self.debug:
            print(txn_seq)

        print(len(txn_seq))

        overall_cost = 0
        if opt_cost:
            overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        else:
            overall_cost = self.get_seq_cost(txn_seq, 0)
        if num_clusters == 1:
            print("greedy cost: ", overall_cost)
        else:
            print("greedy cost constrained: ", overall_cost)

    def get_greedy_cost_opp(self, opt_cost):
        txn_seq = []
        indicies = random.sample(range(self.num_txns), self.num_txns)
        running_cost = 0
        for i in range(self.num_txns):
            t = indicies.pop(0)
            if len(txn_seq) == 0:
                txn_seq.append(t)
                continue
            min_cost = 100000 # MAX
            min_relative_cost = 10
            insert_index = -1
            for i in range(len(txn_seq)):
                test_seq = txn_seq.copy()
                test_seq.insert(i, t)
                if opt_cost:
                    cost = self.get_opt_seq_cost(test_seq, 0)
                else:
                    cost = self.get_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    insert_index = i
            # running_cost = self.txns[t][0][3]
            txn_seq.insert(insert_index, t)
        if self.debug:
            print(txn_seq)
        # print(txn_seq)

        overall_cost = 0
        if opt_cost:
            overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        else:
            overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost opp: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost_opp_sampled(self, num_samples, sample_rate, opt_cost):
        txn_seq = []
        indicies = random.sample(range(self.num_txns), self.num_txns) #[x for x in range(self.num_txns)]#
        running_cost = 0
        for i in range(self.num_txns):
            t = indicies.pop(0)
            if len(txn_seq) == 0:
                txn_seq.append(t)
                continue

            sample = random.random()
            if sample > sample_rate:
                txn_seq.append(t)
                continue

            min_cost = 100000 # MAX
            min_relative_cost = 10
            insert_index = -1
            idx = txn_seq
            if len(txn_seq) > num_samples:
                idx = random.sample(range(len(txn_seq)), num_samples)
            for i in idx:
                test_seq = txn_seq.copy()
                test_seq.insert(i, t)
                if opt_cost:
                    cost = self.get_opt_seq_cost(test_seq, 0)
                else:
                    cost = self.get_seq_cost(test_seq, 0)
                if cost < min_cost:
                    min_cost = cost
                    insert_index = i
            # running_cost = self.txns[t][0][3]
            txn_seq.insert(insert_index, t)
        if self.debug:
            print(txn_seq)
        # print(txn_seq)

        overall_cost = 0
        if opt_cost:
            overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        else:
            overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost opp: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost(self, opt_cost):
        # greedy with random starting point
        start_txn = random.randint(0, self.num_txns - 1)
        txn_seq = [start_txn]
        # key_map, total_cost = self.get_incremental_seq_cost(start_txn, {}, 0)
        running_cost = self.txns[start_txn][0][3]
        for i in range(0, self.num_txns - 1):
            min_cost = 100000 # MAX
            # zmin_cost = 100000
            min_relative_cost = 10
            # zmin_relative_cost = 10
            min_txn = -1
            # zmin_txn = -1
            # min_index = 0
            txns_left = []
            key_maps = []
            ids = random.sample(range(self.num_txns), self.num_txns)
            for j in ids: #range(0, self.num_txns):
                if j in txn_seq:
                    key_maps.append((0,0))
                    continue
                txn_len = self.txns[j][0][3]
                test_seq = txn_seq.copy()
                test_seq.append(j)
                cost = 0
                if opt_cost:
                    cost = self.get_opt_seq_cost(test_seq, 0)
                else:
                    cost = self.get_seq_cost(test_seq, 0)
                    # s_key_map, s_total_cost = self.get_incremental_seq_cost(j, copy.deepcopy(key_map), total_cost)
                    # key_maps.append((s_key_map, s_total_cost))
                    # cost = s_total_cost
                relative_cost = (cost - running_cost * 1.0) / txn_len
                # print(j, relative_cost, cost, running_cost, txn_len)
                # if cost < min_cost:
                if relative_cost < min_relative_cost: #relative_cost > 0 and
                    min_cost = cost
                    min_txn = j
                    min_relative_cost = relative_cost
                    # min_index = j
                # if relative_cost < zmin_relative_cost:
                #     zmin_cost = cost
                #     zmin_txn = j
                # print(min_cost, min_txn)
                # txns_left.append(j)
            # if min_txn == -1:
            #     min_txn = zmin_txn
            assert(min_txn != -1)
            running_cost = min_cost
            # if random.random() < 0.5:
            #     rand_idx = random.randint(0,len(txns_left)-1)
            #     txn_seq.append(txns_left[rand_idx])
            # else:
            #     txn_seq.append(min_txn)
            txn_seq.append(min_txn)
            # key_map, total_cost = key_maps[min_index]
        if self.debug:
            print(txn_seq)
        # print(txn_seq)
        # print(txn_seq, self.get_opt_seq_cost(txn_seq, 0))
        # print("seq:", self.get_random_seq_cost(txn_seq, True, True))

        overall_cost = 0
        if opt_cost:
            overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        else:
            overall_cost = self.get_seq_cost(txn_seq, 0)
        print("greedy cost hey: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost_sampled(self, num_samples, sample_rate, opt_cost):
         # greedy with random starting point
        start_txn = random.randint(0, self.num_txns - 1)
        txn_seq = [start_txn]
        remaining_txns = [x for x in range(0, self.num_txns)]
        remaining_txns.remove(start_txn)
        running_cost = self.txns[start_txn][0][3]
        # key_map, total_cost = self.get_incremental_seq_cost(start_txn, {}, 0)
        for i in range(0, self.num_txns - 1):
            min_cost = 100000 # MAX
            min_relative_cost = 10
            min_txn = -1
            # min_index = 0
            holdout_txns = []
            done = False
            key_maps = []

            sample = random.random()
            if sample > sample_rate:
                idx = random.randint(0, len(remaining_txns) - 1)
                t = remaining_txns[idx]
                txn_seq.append(t)
                remaining_txns.pop(idx)
                continue

            for j in range(0, num_samples):
                idx = 0
                if len(remaining_txns) > 1:
                    idx = random.randint(0, len(remaining_txns) - 1)
                else:
                    done = True
                t = remaining_txns[idx]
                holdout_txns.append(remaining_txns.pop(idx))
                if self.debug:
                    print(remaining_txns, holdout_txns)
                txn_len = self.txns[t][0][3]
                test_seq = txn_seq.copy()
                test_seq.append(t)
                cost = 0
                if opt_cost:
                    cost = self.get_opt_seq_cost(test_seq, 0)
                else:
                    cost = self.get_seq_cost(test_seq, 0)
                    # print(self.get_seq_cost(test_seq, 0))
                    # s_key_map, s_total_cost = self.get_incremental_seq_cost(t, copy.deepcopy(key_map), total_cost)
                    # key_maps.append((s_key_map, s_total_cost))
                    # cost = s_total_cost
                relative_cost = (cost - running_cost * 1.0) / txn_len
                # print(t, relative_cost, cost, running_cost, txn_len)
                # if cost < min_cost:
                if relative_cost < min_relative_cost:
                    min_cost = cost
                    min_txn = t
                    min_relative_cost = relative_cost
                    # min_index = j
                if done:
                    break
            assert(min_txn != -1)
            running_cost = min_cost
            txn_seq.append(min_txn)
            holdout_txns.remove(min_txn)
            remaining_txns.extend(holdout_txns)
            # key_map, total_cost = key_maps[min_index]
            # if self.debug:
            #     print(txn_seq, key_map, total_cost)

            if self.debug:
                print("min: ", min_txn, remaining_txns, holdout_txns, txn_seq)
        if self.debug:
            print(txn_seq)
            print(len(set(txn_seq)))
        # print(txn_seq)

        overall_cost = 0
        if opt_cost:
            overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        else:
            overall_cost = self.get_seq_cost(txn_seq, 0)

        # print(txn_seq, self.get_seq_cost(txn_seq, 0))
        # print("seq:", self.get_random_seq_cost(txn_seq, False, True))
        print("greedy cost sampled: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost_distr_sampled(self, num_samples, num_txn_samples, sample_rate, opt_cost):
        min_txn_seq = []
        min_cost = 100000 # MAX
        # cost_map = {}
        # for i in range(num_samples):
        #     cost, txn_seq = self.get_greedy_cost_opp_sampled(num_txn_samples, sample_rate, opt_cost) #get_greedy_cost_sampled(num_txn_samples, sample_rate, opt_cost)
        #     if cost not in cost_map:
        #         cost_map[cost] = 1
        #     else:
        #         cost_map[cost] += 1
        #     if cost < min_cost:
        #         min_cost = cost
        #         min_txn_seq = txn_seq
        #     if i % 50 == 0:
        #         print("sample: ", i, " min_cost: ", min_cost)
        # print("greedy cost min: ", min_cost)

        # od = collections.OrderedDict(sorted(cost_map.items()))
        # print(od.keys())
        # print(od.values())

        # min_cost = 100000 # MAX
        cost_map = {}
        for i in range(num_samples):
            cost, txn_seq = self.get_greedy_cost_sampled(num_txn_samples, sample_rate, opt_cost) #get_greedy_cost_opp_sampled(num_txn_samples, sample_rate, opt_cost) #
            if cost not in cost_map:
                cost_map[cost] = 1
            else:
                cost_map[cost] += 1
            if cost < min_cost:
                min_cost = cost
                min_txn_seq = txn_seq
            if i % 50 == 0:
                print("sample: ", i, " min_cost: ", min_cost)
        print("greedy cost min: ", min_cost)

        od = collections.OrderedDict(sorted(cost_map.items()))
        print(od.keys())
        print(od.values())

        return min_txn_seq


    def get_greedy_cost_distr(self, num_samples, opt_cost):
        min_cost = 100000 # MAX
        min_txn_seq = []
        cost_map = {}
        for i in range(num_samples):
            cost, txn_seq = self.get_greedy_cost(opt_cost) #get_greedy_cost_opp(opt_cost) #
            if cost not in cost_map:
                cost_map[cost] = 1
            else:
                cost_map[cost] += 1
            if cost < min_cost:
                min_cost = cost
                min_txn_seq = txn_seq
            # if i % 50 == 0:
            #     print("sample: ", i, " min_cost: ", min_cost)
        print("greedy cost min: ", min_cost)

        od = collections.OrderedDict(sorted(cost_map.items()))
        print(od.keys())
        print(od.values())

        return min_txn_seq

    def get_costs(self):
        if not self.materialize:
            print("not materialized")
            return
        for i in range(len(self.sequences)):
            self.get_seq_cost(self.sequences[i], i)

        for c, o in zip(self.sequence_costs, self.sequence_overlaps):
            self.sequence_overall.append(c-o)

        if self.debug:
            print(self.cost_matrix)
            print(self.sequences)
            print(self.sequence_costs)
            print(self.sequence_overlaps)
            print(self.sequence_overall)

        min_cost = min(self.sequence_overall)
        min_len = min(self.sequence_costs)
        min_index = list(self.sequence_costs).index(min_len)
        # min_index = self.sequence_overall.index(min_cost)

        tot_cost = 0.0
        for i, j in zip(self.sequences, self.sequence_costs):
            # print(i, j)
            tot_cost += j
        print("min seq: ", self.sequences[min_index], "cost: ", min_len, " overlap: ", min_cost)
        print("avg cost: ", tot_cost / len(self.sequences))
        max_len = max(self.sequence_costs)
        max_index = list(self.sequence_costs).index(max_len)
        print("max seq: ", self.sequences[max_index], "cost: ", max_len)

        cost_map = {}
        for i in self.sequence_costs:
            if i not in cost_map:
                cost_map[i] = 1
            else:
                cost_map[i] += 1
        od = collections.OrderedDict(sorted(cost_map.items()))
        print(od.keys())
        print(od.values())

        # print_str = "["
        # for i in self.sequence_costs:
        #     print_str += str(i) + ","
        # print_str += "]"
        # print(print_str)
        # print(len(self.sequence_costs))

        # x = self.sequence_costs
        # x.sort()
        # print_str = "[ "
        # for i in x:
        #     print_str += str(i) + " "
        # print_str += " ]"
        # print(print_str)

        # print(self.sequences[min_index], "cost: ", min_cost)

        # costs = np.array(self.sequence_overall)
        # seqs = np.where(costs == min_cost)[0].tolist()
        # for i in seqs:
        #     print(self.sequences[i], "cost: ", min_cost)

        return min_cost, min_len

    def get_opt_costs(self):
        if not self.materialize:
            print("not materialized")
            return
        for i in range(len(self.sequences)):
            self.get_opt_seq_cost(self.sequences[i], i)

        if self.debug:
            print(self.opt_cost_matrix)
            print(self.sequences)
            print(self.opt_sequence_costs)

        min_len = min(self.opt_sequence_costs)
        min_index = list(self.opt_sequence_costs).index(min_len)
        # min_index = self.sequence_overall.index(min_cost)

        tot_cost = 0.0
        for i, j in zip(self.sequences, self.opt_sequence_costs):
            # print(i, j)
            tot_cost += j
        print("min seq: ", self.sequences[min_index], "cost: ", min_len)
        print("avg cost: ", tot_cost / len(self.sequences))

        max_len = max(self.opt_sequence_costs)
        max_index = list(self.opt_sequence_costs).index(max_len)
        print("max seq: ", self.sequences[max_index], "cost: ", max_len)

        cost_map = {}
        for i in self.opt_sequence_costs:
            if i not in cost_map:
                cost_map[i] = 1
            else:
                cost_map[i] += 1
        od = collections.OrderedDict(sorted(cost_map.items()))
        print(od.keys())
        print(od.values())

        # print_str = "["
        # for i in self.opt_sequence_costs:
        #     print_str += str(i) + ","
        # print_str += "]"
        # print(print_str)
        # print(len(self.opt_sequence_costs))

        return min_len

    def get_random_seq_cost(self, seq, opt_cost, print_val):
        min_len = 0
        if opt_cost:
            min_len = self.get_opt_seq_cost(seq, 0)
        else:
            min_len = self.get_seq_cost(seq, 0)
        # print("seq: ", seq, "cost: ", min_len)
        if print_val:
            print("rand seq cost: ", min_len)

        return min_len

    def get_random_costs(self, num_seqs, opt_cost):
        if self.materialize:
            # print("total num seqs: ", len(self.sequences))

            indicies = random.sample(range(len(self.sequences)), num_seqs)
            costs = []
            for i in indicies:
                if opt_cost:
                    costs.append(self.get_opt_seq_cost(self.sequences[i], i))
                else:
                    costs.append(self.get_seq_cost(self.sequences[i], i))

            min_len = min(costs)
            min_index = list(costs).index(min_len)

            tot_cost = 0.0
            for i, j in zip(indicies, costs):
                # print(self.sequences[i], j)
                tot_cost += j
            # print("min seq: ", self.sequences[min_index], "cost: ", min_len)
            # print("avg cost: ", tot_cost / len(self.sequences))

            max_len = max(costs)
            max_index = list(costs).index(max_len)
            # print("max seq: ", self.sequences[max_index], "cost: ", max_len)
        else:
            tot_seqs = math.factorial(len(self.txns))
            # print("total num seqs: ", tot_seqs)

            sequences = []
            costs = []
            for i in range(num_seqs):
                seq = list(np.random.permutation(len(self.txns)))
                sequences.append(seq)
                if opt_cost:
                    costs.append(self.get_opt_seq_cost(seq, i))
                else:
                    costs.append(self.get_seq_cost(seq, i))
                # if i % 100 == 0:
                #     print("seq: ", i)

            min_len = min(costs)
            min_index = list(costs).index(min_len)

            tot_cost = 0.0
            for i, j in zip(sequences, costs):
                # print(sequences[i], j)
                tot_cost += j
            print("min seq: ", sequences[min_index], "cost: ", min_len)
            print("min seq cost: ", min_len)
            print("avg cost: ", tot_cost / len(sequences))

            max_len = max(costs)
            max_index = list(costs).index(max_len)
            # print("max seq: ", sequences[max_index], "cost: ", max_len)
            print("max seq cost: ", max_len)

            cost_map = {}
            for i in costs:
                if i not in cost_map:
                    cost_map[i] = 1
                else:
                    cost_map[i] += 1
            od = collections.OrderedDict(sorted(cost_map.items()))
            print(od.keys())
            print(od.values())

        return min_len




if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", help="batch size")
    parser.add_argument("-p", action="store_true", help="print workload")
    parser.add_argument("-hk", help="number of hot keys")
    args = parser.parse_args()

    start_time = time.time()

    batch = 100
    if len(sys.argv) > 1:
        batch = int(args.b)

    workloadW = '{"txn1": "r-x * * * w-z", "txn2": "r-z * * * w-x"}'
    # workloadW = '{"txn1": "r-x * * * w-x * * *", "txn2": "r-x * * * w-x"}'
    # workloadW = '{"txn1": "r-x w-z * * *", "txn2": "r-z * * * w-z"}'
    # workloadW = '{"txn1": "r-x * * * w-x", "txn2": "r-z * * * w-x"}'
    # workloadW = '{"txn1": "w-x w-y r-y r-y r-y", "txn2": "* r-y * * w-y w-x"}'
    # workloadW = '{"txn1": "w-y w-y w-y", "txn2": "r-y r-y"}'
    # workloadW = '{"txn1": "* * w-y w-y w-y", "txn2": "* r-y w-y"}' # RC vs. SI
    # workloadW = '{"txn1": "w-y w-y w-y", "txn2": "r-y r-y", "txn3": "w-y w-y w-y"}'
    # workloadW = '{"txn1": "w-y w-x w-y", "txn2": "w-x w-y", "txn3": "r-y"}' # RC vs. SI
    # workloadW = '{"txn1": "w-y w-x w-y", "txn2": "* w-x w-y", "txn3": "r-y w-y"}'
    # workloadW = '{"txn1": "r-x r-x w-x", "txn2": "r-x w-x", "txn3": "r-x w-x", "txn4": "w-x r-x"}' # RC vs. SI
    # workloadW = '{"txn0":"r-1 r-1 r-44 w-1 r-1 w-4 r-1 w-1 w-1 w-5", "txn1":"w-2 r-2 r-1 r-2 r-3 r-1 w-1 w-2 w-4 r-3", "txn2":"r-5 w-2 w-3 w-1 w-2 w-1 w-2 r-4 w-1 r-1"}'
    # workloadW = '{"txn1": "* * * * w-x", "txn2": "* * w-x *"}'
    # workloadW = '{"txn1": "w-x * w-x", "txn2": "r-x"}'


    if args.p:
        print(workloadW)

    workload = ComboWorkload(workloadW, False)

    workload.get_random_workloads(3000, False)
    # avg_min = 0
    # min_min = 100000
    # vals = []
    # for i in range(3000):
    #     front_seq = [x for x in range((int(batch/2)))]
    #     back_seq = [x for x in range(int(batch/2), batch)]
    #     seq = random.sample(front_seq, len(front_seq))
    #     seq.extend(random.sample(back_seq, len(back_seq)))
    #     # print(front_seq, back_seq, seq)
    #     val = workload.get_rand_workload_seq(seq, False)
    #     avg_min += val
    #     min_min = min(min_min, val)
    #     vals.append(val)
    # cost_map = {}
    # for i in vals:
    #     if i not in cost_map:
    #         cost_map[i] = 1
    #     else:
    #         cost_map[i] += 1
    # od = collections.OrderedDict(sorted(cost_map.items()))
    # print(od.keys())
    # print(od.values())
    # print("min constrained seqs: ", min_min)
    workload.get_rand_workload_seq([batch - 1 - x for x in range(batch)], False, True)
    # print([x for x in range(batch)])
    workload.get_rand_workload_seq([x for x in range(batch)], False, True)
    # txn_seq = []
    # for i in range(int(batch/2)):
    #     txn_seq.append(i)
    #     txn_seq.append(i+int(batch/2))
    # workload.get_rand_workload_seq(txn_seq, False, True)
    # workload.get_greedy_cost_constrained(1, False)
    # # # workload.get_random_sub_costs(10, 3000, False)
    # # workload.get_rand_workload_seq([6, 4, 2, 0, 7, 5, 1, 3], True)
    # # workload.get_rand_workload_seq([9,8,7,6,5,4,3,2,1,0], True)
    # # workload.get_rand_workload_seq([9,8,7,6,5,4,3,2,1,0,19,18,17,16,15,14,13,12,11,10], True)
    # # workload.get_rand_workload_seq([19,18,17,16,15,9,8,7,6,5,14,13,12,11,10,4,3,2,1,0], True)
    # workload.get_rand_workload_seq([28, 1, 30, 2, 31, 6, 37, 7, 10, 16, 20, 22, 8, 21, 11, 23, 15, 24, 18, 26, 27, 29, 25, 32, 34, 35, 3, 4, 5, 9, 13, 14, 0, 12, 17, 33, 36, 38, 39, 19], False, True)
    # # workload.get_rand_workload_seq([1,0], True)
    # workload.get_rand_workload_seq([7, 108, 1, 100, 112, 3, 101, 9, 122, 5, 105, 10, 123, 6, 106, 11, 124, 13, 110, 18, 130, 14, 113, 20, 133, 15, 114, 22, 134, 19, 115, 23, 137, 21, 120, 27, 140, 26, 121, 28, 144, 30, 125, 29, 146, 33, 126, 31, 148, 36, 127, 32, 160, 39, 131, 34, 161, 45, 138, 35, 172, 46, 139, 40, 176, 56, 141, 41, 178, 60, 147, 49, 184, 65, 149, 50, 185, 66, 152, 53, 192, 67, 153, 54, 194, 71, 162, 61, 75, 163, 63, 79, 164, 78, 82, 168, 83, 102, 0, 103, 2, 109, 4, 104, 8, 116, 12, 107, 17, 118, 16, 111, 38, 119, 24, 117, 48, 129, 25, 128, 55, 132, 37, 135, 58, 136, 42, 151, 68, 142, 43, 157, 69, 143, 44, 159, 73, 145, 47, 166, 81, 150, 51, 169, 87, 154, 52, 170, 92, 155, 57, 177, 94, 156, 59, 179, 96, 158, 62, 180, 99, 165, 64, 181, 167, 70, 187, 171, 72, 190, 173, 74, 198, 174, 76, 186, 77, 188, 80, 189, 85, 193, 86, 84, 88, 175, 93, 182, 89, 98, 183, 91, 191, 95, 195, 97, 197, 199, 196, 90], False, True)
    # # workload.get_rand_workload_seq([8,6,4,2,0,10,11,9,7,5,3,1])
    # # workload.get_rand_workload_seq([17,15,13,11,9,7,5,3,1,16,14,12,10,8,6,4,2,0])
    # # workload.get_rand_workload_seq([1,3,5,7,9,11,13,15,17,16,14,12,10,8,6,4,2,0])
    # # workload.get_random_sub_costs(1, 30000, True)
    # print("======================================")
    # workload.get_fsj_cost()
    # workload.get_greedy_opt_cost()
    # workload.get_greedy_cost_constrained(2, True)
    # workload.get_greedy_cost_distr(2, True)
    workload.get_random_workloads(3000, True)

    # workload.get_greedy_cost_constrained(1, True)
    # print("time: ", time.time() - start_time)

    # workload.get_greedy_cost_distr_sampled(10, 10, 1.0, True)
    # print("time: ", time.time() - start_time)
    # workload.get_greedy_cost_distr_sampled(5, 50, 1.0, True)
    # print("time: ", time.time() - start_time)
    # workload.get_greedy_cost_distr_sampled(10, 100, 1.0, True)
    # print("time: ", time.time() - start_time)

    # workload.get_greedy_cost_distr_sampled(20, 50, 1.0, True)
    # workload.get_greedy_cost_sampled(10, 1.0, True)
    # print("time: ", time.time() - start_time)
    # workload.get_greedy_cost_sampled(50, 1.0, True)
    # print("time: ", time.time() - start_time)
    # workload.get_greedy_cost_sampled(90, 1.0, True)
    # print("time: ", time.time() - start_time)
    # workload.get_greedy_cost_constrained(1, True)
    # # seq = []
    # # end_seq = []
    # # for x in range(int(batch/10)):
    # #     for j in range(0,5):
    # #         seq.append(x*10+j)
    # #         end_seq.append(x*10+j+5)
    # # seq.extend(end_seq)
    # # # print(seq)
    # # workload.get_rand_workload_seq(seq, True)
    # # # workload.get_rand_workload_seq([0,1,2,3,4,10,11,12,13,14,5,6,7,8,9,15,16,17,18,19], True)
    # workload.get_rand_workload_seq([2, 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29], True, True)
    # workload.get_rand_workload_seq([22, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29], True, True)
    # workload.get_rand_workload_seq([28, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 29], True, True)
    # avg_min = 0
    # min_min = 100000
    # vals = []
    # for i in range(3000):
    #     front_seq = [x for x in range((int(batch/2)))]
    #     back_seq = [x for x in range(int(batch/2), batch)]
    #     seq = random.sample(front_seq, len(front_seq))
    #     seq.extend(random.sample(back_seq, len(back_seq)))
    #     # print(front_seq, back_seq, seq)
    #     val = workload.get_rand_workload_seq(seq, True)
    #     avg_min += val
    #     min_min = min(min_min, val)
    #     vals.append(val)
    # cost_map = {}
    # for i in vals:
    #     if i not in cost_map:
    #         cost_map[i] = 1
    #     else:
    #         cost_map[i] += 1
    # od = collections.OrderedDict(sorted(cost_map.items()))
    # print(od.keys())
    # print(od.values())
    # print("min constrained seqs: ", min_min)
    workload.get_rand_workload_seq([batch - 1 - x for x in range(batch)], True, True)
    workload.get_rand_workload_seq([x for x in range(batch)], True, True)
    # txn_seq = []
    # for i in range(int(batch/2)):
    #     txn_seq.append(i)
    #     txn_seq.append(i+int(batch/2))
    # workload.get_rand_workload_seq(txn_seq, True, True)
    # # seq = []
    # # for x in range(int(batch/2)):
    # #     seq.append(x)
    # #     if not batch - x == batch:
    # #         seq.append(batch - x)
    # # print(seq)
    # # seq = [x for x in range(0,100,2)]
    # # seq.extend([x for x in range(1,100,2)])
    # # workload.get_rand_workload_seq(seq, True)
    # # seq2 = [x for x in range(1,100,2)]
    # # seq2.extend([x for x in range(0,100,2)])
    # # workload.get_rand_workload_seq(seq2, True)
    # # workload.get_rand_workload_seq([0,1], True)
    print("time: ", time.time() - start_time)


# overlap = total_cost
        # curr_pos = 0
        # prev_txn = txn_seq[0]
        # running_cost = 0
        # for i in range(len(txn_seq)):
        #     txn = self.txns[txn_seq[i]]
        #     txn_start = 1
        #     curr_txn = txn_seq[i]
        #     running_cost += self.cost_matrix[i][prev_txn,curr_txn]
        #     for j in range(len(txn)):
        #         (op_type, key, pos, txn_len) = txn[j]
        #         curr_pos = txn_len - self.cost_matrix[i][prev_txn,curr_txn] # TODO: fix this; should be additive pos
        #         print("cost: ", self.cost_matrix[i][prev_txn,curr_txn], running_cost)
        #         temp_pos = curr_pos + pos # account for where this key is in overall sequence
        #         assert key in key_map
        #         key_start = 0
        #         if key_map[key][-1][0] == 'w' or op_type == 'w':
        #             key_start = key_map[key][-1][2] + 1 # get end time of latest lock end
        #         else:
        #             key_start = temp_pos # read locks shared
        #         txn_start = max(txn_start, key_start - temp_pos + 1)
        #         if total_cost - txn_start >= 0: # check if there is overlap
        #             overlap = min(overlap, total_cost - txn_start + 1)
        #         elif total_cost - txn_start < 0: # no overlap possible
        #             overlap = 0
        #             break
        #         if self.debug:
        #             print(txn, key, curr_pos, key_start, pos, temp_pos, txn_start)
        #             print(overlap,total_cost,txn_start)
        #     prev_txn = curr_txn
