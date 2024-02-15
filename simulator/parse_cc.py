from itertools import combinations, permutations
from scipy import stats
from scipy.stats import zipf
from scipy.cluster.vq import whiten
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.decomposition import PCA

from parse_workload import ComboWorkload

import argparse
import collections
import copy
import json
import math
import numpy as np
import random
import sys
import time

class Workload:
    def __init__(self, workload_json, cutoff, debug=False, verify=False): #cluster_json=None,
        self.cutoff = cutoff
        self.workload = list(json.loads(workload_json).values())[:self.cutoff]
        # print(self.workload)
        self.num_txns = len(self.workload)
        self.debug = debug
        self.verify = verify
        self.txns = [] # list of txns (list of ops)
        self.only_hot_keys = False#True#
        self.hot_keys_thres = 500

        # self.txn_clusters = [] # list of clusters which txns belong to
        # if cluster_json != None:
        #     self.txn_clusters = list(json.loads(cluster_json).values())

        self.get_txns()

    # get transactions from json and represent hot keys as (r/w, key, position, txn_len)
    def get_txns(self):
        for txn in self.workload: #.values()
            txn_ops = []
            ops = txn.split(' ')
            txn_len = len(ops)
            skip_txn = False
            count = 0
            tmp1 = None
            tmp2 = None
            tmp3 = None
            for i in range(len(ops)):
                op = ops[i]
                if op != '*':
                    vals = op.split('-')
                    if len(vals) != 2:
                        print(op, vals)
                    assert len(vals) == 2
                    # if i == 0 and int(vals[1]) > 500:
                    #     skip_txn = True
                    #     break
                    if self.only_hot_keys and int(vals[1]) > self.hot_keys_thres:
                        tmp1 = vals[0]
                        tmp2 = vals[1]
                        tmp3 = i+1
                        continue
                    else:
                        count += 1
                    txn_ops.append((vals[0], vals[1], i+1, len(ops)))
            if count == 0 and self.only_hot_keys:
                txn_ops.append((tmp1, tmp2, tmp3, len(ops)))
            # if skip_txn:
            #     continue
            self.txns.append(txn_ops)
        if self.debug:
            print(self.txns)
        self.num_txns = len(self.txns)

    # get all possible sequences of transactions
    def get_all_sequences(self, cost_type):
        sequences = list(permutations([i for i in range(len(self.txns))]))
        costs = []
        for s in sequences:
            cost = self.get_cost(s, cost_type)
            costs.append(cost)

        min_len = min(costs)
        min_index = list(costs).index(min_len)

        tot_cost = 0.0
        for i, j in zip(sequences, costs):
            # print(sequences[i], j)
            tot_cost += j
        # print("min seq: ", sequences[min_index], "cost: ", min_len)
        print("total number of seqs: ", len(sequences))
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


    def get_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
        key_map = {} # <key, [(r/w, lock_start, lock_end)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        txn_id = 0
        for i in range(len(txn_seq)):
            time = i
            txn = self.txns[txn_seq[i]]
            if self.debug:
                print(txn)
            txn_start = 1
            txn_total_len = 0
            max_release = 0
            cost = 0
            writes_before_reads = [] # special case of reads after writes to same txn (read locks cannot be shared)
            wr_ops = [] # indicies of reads after writes
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
                if op_type == 'w':
                    writes_before_reads.append(key)
                elif key in writes_before_reads:
                    wr_ops.append(j)
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
            prev_txn = curr_txn
            if self.debug:
                print(txn_start, txn_end, max_release, cost)

            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = txn_start + pos - 1
                if j in wr_ops: # if a read is after a write, txn still holds write lock so pretend read is a w
                    op_type = 'w'
                if key in key_map:
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        self.insert_key_map(key, key_map, op_type, key_start, txn_end, txn_id)
                        # key_map[key].append((op_type, key_start, txn_end, txn_id))
                    else: # make sure read lock is held for appropriate duration
                        self.insert_key_map(key, key_map, op_type, key_start, max(txn_end, key_map[key][-1][2]), txn_id)
                        # key_map[key].append((op_type, key_start, max(txn_end, key_map[key][-1][2]), txn_id))
                else:
                    key_map[key] = [(op_type, key_start, txn_end, txn_id)]
                if self.verify:
                    if key_start > txn_end:
                        print(key, key_start, txn_end, key_map[key])
                        assert False
            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            print(total_cost)

        if self.verify:
            for key, ops in key_map.items():
                key_reads = {}
                key_writes = {} # <key, list of (txn_id, write indices)>
                for op in ops:
                    (op_type, key_start, key_end, tid) = op
                    vals = [x for x in range(key_start, key_end + 1)]
                    for v in vals:
                        if op_type == 'w':
                            if key in key_writes: # check writes don't overlap with writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                # if v in key_writes[key] and idv != tid:
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                # if idv != tid:
                                    # assert v not in key_writes[key]
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                            if key in key_reads: # check writes don't overlap with reads
                                matches = [idx for i, (idx, pos) in enumerate(key_reads[key]) if pos == v]
                                # if v in key_reads[key] and idv != tid:
                                #     print(v, key, key_reads[key], op, key_map[key])
                                # if idv != tid:
                                #     assert v not in key_reads[key]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_reads[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                key_reads[key].append((tid, v))
                        else:
                            if key in key_writes: # check that reads don't overlap writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                # if v in key_writes[key] and idv != tid:
                                #     print(v, key, key_writes[key], op, key_map[key])
                                #     print(txn_seq)
                                # if idv != tid:
                                #     assert v not in key_writes[key]
                            if key in key_reads:
                                key_reads[key].append((tid, v))
                            else:
                                key_reads[key] = [(tid, v)]
                    for v in vals: # add write locks after checking
                        if op_type == 'w':
                            if key in key_writes:
                                key_writes[key].append((tid, v))
                            else:
                                key_writes[key] = [(tid, v)]

        return total_cost

    # get the earliest free slot for a write accounting for lock hold duration
    def find_earliest_write_rc(self, key, key_map, pos, duration, txn_end, txn_id, gaps_map): #
        if self.debug:
            print(key, key_map[key], pos, duration, gaps_map)
        index = 0
        running_count = pos
        while index < len(key_map[key]):
            if key_map[key][index][1] > running_count + duration and key_map[key][index][1] > txn_end:
                break
            if key_map[key][index][3] == txn_id: # as we're adding to key_map
                running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
                # don't need to break for rc since we don't have max_txn_id as a safety barrier, checks above needed
            else:
                running_count = max(key_map[key][index][2] + 1, running_count)
            index += 1


        # if key in gaps_map:
        #     index = -1
        #     for i in range(len(gaps_map[key])):
        #         (s, e, typ) = gaps_map[key][i]
        #         if e - s + 1 >= duration and s >= running_count and typ == 'w':
        #             running_count = s
        #             index = i
        #             break
        #     if index == -1:
        #         index = len(key_map[key]) - 1
        #         while index >= 0:
        #             if key_map[key][index][0] == 'w':
        #                 running_count = max(key_map[key][index][2] + 1, running_count)
        #                 break
        #             index -= 1
        #     # else:
        #     #     gaps_map[key].pop(i)

        if self.debug:
            print("running_count: ", running_count)
        return running_count

    # def add_gaps_map(self, key, key_start, key_end, op_type, gaps_map):
    #     if self.debug:
    #         print("add_gaps_map: ", key, key_start, key_end, op_type, gaps_map)
    #     if key in gaps_map:
    #         gaps = gaps_map[key]
    #         if op_type == 'w':
    #             for i in range(len(gaps)):
    #                 (s,e,typ) = gaps[i]
    #                 if s == key_start and typ == op_type:
    #                     if e == key_end:
    #                         gaps.pop(i)
    #                     else:
    #                         gaps[i] = (key_end + 1, e, typ)
    #                     break
    #         else:
    #             gaps = gaps_map[key]
    #             inserted = False
    #             for i in range(len(gaps)):
    #                 (s,e,typ) = gaps[i]
    #                 if s == key_start:
    #                     if e == key_end:
    #                         gaps[i] = (s,e,op_type)
    #                     else:
    #                         gaps[i] = (key_end + 1, e, typ)
    #                         gaps.insert(i, (s, key_end + 1, op_type))
    #                     inserted = True
    #                     break
    #             if not inserted:
    #                 gaps_map[key].append((key_start, key_end, op_type))
    #         gaps_map[key] = gaps
    #     else:
    #         if op_type == 'w':
    #             if key_start != 1:
    #                 gaps_map[key] = [(1, key_start - 1, op_type)]
    #         else:
    #             gaps_map[key] = [(key_start, key_end, op_type)]

    # find earliest read slot
    def find_earliest_read_rc(self, key, key_map, pos, txn_id, gaps_map): # TODO: earliest read as long as not part of same txn?? NO, b/c txn deps
        if self.debug:
            print(key, key_map[key], pos, txn_id, gaps_map)
        index = 0
        running_count = pos
        curr_pos = pos
        prev_txn_id = -1
        while index < len(key_map[key]):
            if curr_pos < key_map[key][index][1] and key_map[key][index][3] != prev_txn_id: # no reads here yet
                break
            if key_map[key][index][2] >= running_count:
                if key_map[key][index][0] == 'r':
                    running_count = max(key_map[key][index][1], running_count) # share read lock
                    break
            if key_map[key][index][3] == txn_id: # as we're adding to key_map
                running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
                # don't need to break for rc since we don't have max_txn_id as a safety barrier, checks above needed
            else:
                running_count = max(key_map[key][index][2] + 1, running_count)
            if key_map[key][index][2] + 1 > curr_pos:
                curr_pos = key_map[key][index][2] + 1
                prev_txn_id = key_map[key][index][3]
            index += 1
            # print(running_count)


        # while index < len(key_map[key]):
        #     if key_map[key][index][2] > running_count or key_map[key][index][0] == 'r':
        #         if key_map[key][index][0] == 'w':
        #             running_count = max(key_map[key][index][2] + 1, running_count)
        #         break
        #     if key_map[key][index][3] == txn_id: # as we're adding to key_map
        #         running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
        #     else:
        #         running_count = max(key_map[key][index][2] + 1, running_count)
        #     index += 1

        # if key in gaps_map:
        #     index = -1
        #     for i in range(len(gaps_map[key])):
        #         (s, e, typ) = gaps_map[key][i]
        #         if s >= running_count:
        #             running_count = s
        #             index = i
        #             break
        #     if index == -1:
        #         if key_map[key][-1][3] == txn_id:
        #             running_count = max(key_map[key][-1][1] + 1, running_count) # same txn so only account for this op
        #         else:
        #             running_count = max(key_map[key][-1][2] + 1, running_count)

        if self.debug:
            print("read running_count: ", running_count)
        return running_count

    def get_rc_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
        key_map = {} # <key, [(r/w, lock_start, lock_end)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        txn_id = 0
        gaps_map = {} # <key, [(start, end) of free slots]
        for i in range(len(txn_seq)):
            last_pos = 0
            time = i
            txn = self.txns[txn_seq[i]]
            if self.debug:
                print(txn)
            txn_end = 1
            cost = 0
            prev_key_start = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start
                # if key_start == 0:
                #     key_start = 1
                if self.debug:
                    print("op: ", key, key_start, prev_key_start, pos)
                if key in key_map:
                    if op_type == 'w':
                        key_start = self.find_earliest_write_rc(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_end, txn_id, gaps_map) # have to start after prev. op
                    else:
                        key_start = self.find_earliest_read_rc(key, key_map, key_start, txn_id, gaps_map)
                prev_key_start = key_start
                txn_end = max(txn_end, key_start)
                last_pos = pos
            added_len = 0
            if last_pos != txn_len: # account for residual tail
                txn_end += txn_len - last_pos
                added_len = txn_len - last_pos

            # txn_end might change as we go through so we need to check if it's changed
            curr_txn_end = 0
            tmp_txn_end = txn_end
            while curr_txn_end != txn_end:
                prev_key_start = 0
                last_pos = 0
                for j in range(len(txn)):
                    (op_type, key, pos, txn_len) = txn[j]
                    key_start = pos - last_pos + prev_key_start
                    if self.debug:
                        print("checking: ", key, key_start, prev_key_start, txn[j])
                    if key in key_map:
                        if op_type == 'w': # txn_end - key_start
                            key_start = self.find_earliest_write_rc(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_end, txn_id, gaps_map) # have to start after prev. op
                        else:
                            key_start = self.find_earliest_read_rc(key, key_map, key_start, txn_id, gaps_map)
                    prev_key_start = key_start
                    txn_end = max(txn_end, key_start)
                    last_pos = pos
                if last_pos != txn_len: # account for residual tail
                    txn_end -= added_len
                    txn_end += txn_len - last_pos
                    added_len = txn_len - last_pos
                curr_txn_end = tmp_txn_end
                tmp_txn_end = txn_end

            cost = txn_end - total_cost #max_release
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            total_cost += cost
            if self.debug:
                print("txn end: ", txn_end, cost, total_cost)

            # need to use txn_end
            prev_key_start = 0
            last_pos = 0
            # write_yet = set() # write for key occured yet
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start
                if self.debug:
                    print("adding: ", key, key_start, prev_key_start, txn[j])
                if key in key_map:
                    if op_type == 'w': # txn_end - key_start
                        key_start = self.find_earliest_write_rc(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_end, txn_id, gaps_map) # have to start after prev. op
                    else:
                        key_start = self.find_earliest_read_rc(key, key_map, key_start, txn_id, gaps_map)
                prev_key_start = key_start
                last_pos = pos
                key_end = key_start
                if op_type == 'w':
                    key_end = txn_end

                if key in key_map:
                    self.insert_key_map(key, key_map, op_type, key_start, key_end, txn_id)
                    # key_map[key].append((op_type, key_start, key_end, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_end, txn_id)]

                if self.verify:
                    if key_start > txn_end:
                        print(key, key_start, txn_end, key_map[key])
                        assert False

                # if op_type == 'w':
                #     write_yet.add(key)

                # if key not in write_yet:
                #     self.add_gaps_map(key, key_start, key_end, op_type, gaps_map)

                # if self.debug:
                #     print("gaps_map: ", key, key_start, key_end, gaps_map)

            # for j in range(len(txn)):
            #     (op_type, key, pos, txn_len) = txn[j]
            #     key_start = txn_start + pos - 1
            #     if key in key_map:
            #         if key_map[key][-1][0] == 'w' or op_type == 'w': # TODO: check this
            #             key_map[key].append((op_type, key_start, txn_end))
            #         else: # make sure read lock is held for appropriate duration
            #             key_map[key].append((op_type, key_start, key_map[key][-1][1])) # TODO: check this
            #     else:
            #         key_map[key] = [(op_type, key_start, txn_end)]
            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            # print(gaps_map)
            print(total_cost)

        if self.verify:
            for key, ops in key_map.items():
                key_reads = {}
                key_writes = {} # <key, list of (txn_id, write indices)>
                for op in ops:
                    (op_type, key_start, key_end, tid) = op
                    vals = [x for x in range(key_start, key_end + 1)]
                    for v in vals:
                        if op_type == 'w':
                            if key in key_writes: # check writes don't overlap with writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                # if v in key_writes[key] and idv != tid:
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                # if idv != tid:
                                    # assert v not in key_writes[key]
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                            if key in key_reads: # check writes don't overlap with reads
                                matches = [idx for i, (idx, pos) in enumerate(key_reads[key]) if pos == v]
                                # if v in key_reads[key] and idv != tid:
                                #     print(v, key, key_reads[key], op, key_map[key])
                                # if idv != tid:
                                #     assert v not in key_reads[key]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_reads[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                key_reads[key].append((tid, v))
                        else:
                            if key in key_writes: # check that reads don't overlap writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                # if v in key_writes[key] and idv != tid:
                                #     print(v, key, key_writes[key], op, key_map[key])
                                #     print(txn_seq)
                                # if idv != tid:
                                #     assert v not in key_writes[key]
                            if key in key_reads:
                                key_reads[key].append((tid, v))
                            else:
                                key_reads[key] = [(tid, v)]
                    for v in vals: # add write locks after checking
                        if op_type == 'w':
                            if key in key_writes:
                                key_writes[key].append((tid, v))
                            else:
                                key_writes[key] = [(tid, v)]

        return total_cost

    # get the index of the first in a consecutive seq. of reads
    def find_earliest_read(self, key, key_map, txn_id):
        # must use latest read if part of same txn
        if key_map[key][-1][3] == txn_id: # as we're adding to key_map
            print("TXN_ID")
            return key_map[key][-1][1]
        else:
            if self.debug:
                print(key, key_map[key], txn_id)
            index = len(key_map[key]) - 1
            while key_map[key][index][0] == 'r':
                if index == -1:
                    break
                index -= 1
        if self.debug:
            print("index: ", index)
        if index == -1: # can be first read
            index = 0
        else:
            index = key_map[key][index][2] + 1 # after first write found
        return index

    def get_opt_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
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
            prev_txn = curr_txn
            if self.debug:
                print(txn_start, txn_end, max_release, cost)

            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = txn_start + pos - 1
                if key in key_map:
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        self.insert_key_map(key, key_map, op_type, key_start, key_start, txn_id)
                        # key_map[key].append((op_type, key_start, key_start, txn_id))
                    else:
                        self.insert_key_map(key, key_map, op_type, key_start, key_start, txn_id)
                        # key_map[key].append((op_type, key_start, key_start, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_start, txn_id)]
            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            print(total_cost)

        if self.verify:
            for key, ops in key_map.items():
                key_reads = {}
                key_writes = {} # <key, list of (txn_id, write indices)>
                for op in ops:
                    (op_type, key_start, key_end, tid) = op
                    vals = [x for x in range(key_start, key_end + 1)]
                    for v in vals:
                        if op_type == 'w':
                            if key in key_writes: # check writes don't overlap with writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                # if v in key_writes[key] and idv != tid:
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                # if idv != tid:
                                    # assert v not in key_writes[key]
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                            if key in key_reads: # check writes don't overlap with reads
                                matches = [idx for i, (idx, pos) in enumerate(key_reads[key]) if pos == v]
                                # if v in key_reads[key] and idv != tid:
                                #     print(v, key, key_reads[key], op, key_map[key])
                                # if idv != tid:
                                #     assert v not in key_reads[key]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_reads[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                key_reads[key].append((tid, v))
                        else:
                            if key in key_writes: # check that reads don't overlap writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                # if v in key_writes[key] and idv != tid:
                                #     print(v, key, key_writes[key], op, key_map[key])
                                #     print(txn_seq)
                                # if idv != tid:
                                #     assert v not in key_writes[key]
                            if key in key_reads:
                                key_reads[key].append((tid, v))
                            else:
                                key_reads[key] = [(tid, v)]
                    for v in vals: # add write locks after checking
                        if op_type == 'w':
                            if key in key_writes:
                                key_writes[key].append((tid, v))
                            else:
                                key_writes[key] = [(tid, v)]

        return total_cost

    def get_occ_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
        key_map = {} # <key, [(r/w, lock_start, lock_end, txn_id)]>
        txn_map = {} # <txn_id, txn_end>
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
                        past_txn_id = key_map[key][-1][3]
                        key_start = max(key_start, txn_map[past_txn_id]) # key_map[key][-1][2] + 1 # get end time of latest lock end
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
            prev_txn = curr_txn
            if self.debug:
                print(txn_start, txn_end, max_release, cost)

            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = txn_start + pos - 1
                if key in key_map:
                    if key_map[key][-1][0] == 'w' or op_type == 'w':
                        self.insert_key_map(key, key_map, op_type, key_start, key_start, txn_id)
                        # key_map[key].append((op_type, key_start, key_start, txn_id))
                    else:
                        self.insert_key_map(key, key_map, op_type, key_start, key_start, txn_id)
                        # key_map[key].append((op_type, key_start, key_start, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_start, txn_id)]
            if self.debug:
                print(key_map)
            txn_map[txn_id] = txn_end
            txn_id += 1
        if self.debug:
            print(total_cost)

        return total_cost

    # find the max position of a key
    def find_max_index(self, key, key_map):
        max_index = 1
        for op in key_map[key]:
            (op_type, key_start, key_end, txn_id) = op
            max_index = max(key_end, max_index)
        return max_index

    # find the max position of a key before limit
    def find_max_index_limit(self, key, key_map, limit):
        max_index = 1
        for op in key_map[key]:
            (op_type, key_start, key_end, txn_id) = op
            if key_start < limit:
                max_index = max(key_end, max_index)
        return max_index


    # get the earliest free slot for a write
    def find_earliest_write_ru(self, key, key_map, pos, gaps_map):
        if self.debug:
            print(key, key_map[key], pos, gaps_map)
        index = 0
        running_count = pos
        # while index < len(key_map[key]):
        #     if key_map[key][index][2] > running_count:
        #         if index < len(key_map[key]) - 1:
        #             if key_map[key][index+1][1] > running_count:
        #                 break
        #     running_count = max(key_map[key][index][2] + 1, running_count)
        #     print("while: ", running_count, index)
        #     index += 1

        if key in gaps_map:
            index = -1
            for i in range(len(gaps_map[key])):
                (s, e, typ) = gaps_map[key][i]
                if s <= running_count and e >= running_count and typ == 'w':
                    running_count = max(running_count, s)
                    index = i
                    break
                elif s >= running_count and typ == 'w':
                    running_count = max(running_count, s)
                    index = i
                    break
            if index == -1: # no free write spots so put write at end
                max_index = self.find_max_index(key, key_map)
                running_count = max(max_index + 1, running_count)
                # index = len(key_map[key]) - 1
                # while index >= 0:
                #     if key_map[key][index][0] == 'w':
                #         running_count = max(key_map[key][index][2] + 1, running_count)
                #         break
                #     index -= 1

        if self.debug:
            print("running_count: ", running_count)
        return running_count

    def add_gaps_map(self, key, key_map, key_start, key_end, op_type, gaps_map):
        if self.debug:
            print("add_gaps_map: ", key, key_start, key_end, op_type, gaps_map[key])
        if key in gaps_map:
            gaps = gaps_map[key]
            if op_type == 'w':
                inserted = False
                for i in range(len(gaps)):
                    (s,e,typ) = gaps[i]
                    if s <= key_start and e >= key_end and typ == op_type:
                        inserted = True
                        vals = [x for x in range(s, e + 1)]
                        index = vals.index(key_start)
                        # print("vals: ", vals)
                        if len(vals) == 1:
                            gaps.pop(i)
                            break
                        if index == 0:
                            gaps[i] = (s + 1, e, typ)
                        elif index == len(vals) - 1:
                            gaps[i] = (s, e - 1, typ)
                        else:
                            gaps[i] = (vals[index+1], e, typ)
                            gaps.insert(i, (s, vals[index-1], typ))
                        break
                if not inserted: # must be at the end
                    if len(gaps) == 0: # first write must have been at index 1
                        last_gap_end = self.find_max_index_limit(key, key_map, key_start)
                        if key_start - 1 > last_gap_end:
                            gaps.append((last_gap_end + 1, key_start - 1, op_type))
                    else:
                        last_gap_end = max(gaps[-1][1], self.find_max_index_limit(key, key_map, key_start))
                        if key_start - 1 > last_gap_end:
                            gaps.append((last_gap_end + 1, key_start - 1, op_type))
                    # if s == key_start and typ == op_type:
                    #     if e == key_end:
                    #         gaps.pop(i)
                    #     else:
                    #         gaps[i] = (key_end + 1, e, typ)
                    #     break
            else:
                inserted = False
                for i in range(len(gaps)):
                    (s,e,typ) = gaps[i]
                    if s <= key_start and e >= key_end:
                        inserted = True
                        if typ == op_type: # already a read, nothing needs to be done
                            break
                        else: # remove from write gap
                            vals = [x for x in range(s, e + 1)]
                            index = vals.index(key_start)
                            if len(vals) == 1:
                                gaps[i] = (s, e, op_type)
                                break
                            if index == 0:
                                gaps[i] = (s + 1, e, typ)
                                gaps.insert(i, (key_start, key_end, op_type))
                            elif index == len(vals) - 1:
                                gaps[i] = (s, e - 1, typ)
                                gaps.insert(i + 1, (key_start, key_end, op_type))
                            else:
                                gaps[i] = (vals[index+1], e, typ)
                                gaps.insert(i, (key_start, key_end, op_type))
                                gaps.insert(i, (s, vals[index-1], typ))
                            break
                    # if s == key_start:
                    #     if e == key_end:
                    #         gaps[i] = (s,e,op_type)
                    #     else:
                    #         gaps[i] = (key_end + 1, e, typ)
                    #         gaps.insert(i, (s, key_end, op_type))
                    #     inserted = True
                    #     break
                if not inserted:
                    if len(gaps) == 0: # first write must have been at index 1
                        last_gap_end = self.find_max_index_limit(key, key_map, key_start)
                        if key_start - 1 > last_gap_end:
                            gaps.append((last_gap_end + 1, key_start - 1, 'w'))
                    else:
                        last_gap_end = max(gaps[-1][1], self.find_max_index_limit(key, key_map, key_start))
                        if key_start - 1 > last_gap_end:
                            gaps_map[key].append((last_gap_end + 1, key_start - 1, 'w'))
                    gaps_map[key].append((key_start, key_end, op_type))
                    # if key == '0:78':
                    #     print(key, key_start, self.find_max_index_limit(key, key_map, key_start), key_map[key], gaps_map[key])
            gaps_map[key] = gaps
        else:
            if op_type == 'w':
                if key_start != 1:
                    gaps_map[key] = [(1, key_start - 1, op_type)]
                else:
                    gaps_map[key] = []
            else:
                if key_start != 1:
                    gaps_map[key] = [(1, key_start - 1, 'w'), (key_start, key_start, op_type)]
                else:
                    gaps_map[key] = [(key_start, key_end, op_type)]

        if self.debug:
            print("curr gaps_map: ", gaps_map[key])

    # find earliest read slot
    def find_earliest_read_ru(self, key, key_map, pos, txn_id, gaps_map):
        # if key == '6744' and txn_id == 280:
        #     self.debug = True

        if self.debug:
            print(key, key_map[key], pos, gaps_map[key])
        index = 0
        running_count = pos
        # while index < len(key_map[key]):
        #     if key_map[key][index][2] > running_count or key_map[key][index][0] == 'r':
        #         break
        #     if key_map[key][index][3] == txn_id: # as we're adding to key_map
        #         running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
        #     else:
        #         running_count = max(key_map[key][index][2] + 1, running_count)
        #     index += 1
        if key in gaps_map:
            index = -1
            prev_end = 0
            for i in range(len(gaps_map[key])):
                (s, e, typ) = gaps_map[key][i]
                if s <= running_count and e >= running_count:
                    running_count = max(running_count, s)
                    index = i
                    break
                elif s >= running_count:
                    running_count = s
                    index = i
                    break
                prev_end = e
            if index == -1:
                if key_map[key][-1][3] == txn_id:
                    running_count = max(key_map[key][-1][1] + 1, running_count) # same txn so only account for this op
                else:
                    max_index = self.find_max_index(key, key_map) # no free write spots so put at end
                    running_count = max(max_index + 1, running_count)

        if self.debug:
            print("read running_count: ", running_count)

        # self.debug = False
        return running_count

    def get_ru_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
        key_map = {} # <key, [(r/w, lock_start, lock_end, txn_id)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        txn_id = 0
        gaps_map = {} # <key, [(start, end) of free slots]
        for i in range(len(txn_seq)):
            time = i
            txn = self.txns[txn_seq[i]]
            txn_end = 1
            cost = 0
            prev_key_start = 0
            last_pos = 0
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start #pos
                if self.debug:
                    print(key, key_start, prev_key_start)
                if key in key_map:
                    if op_type == 'w':
                        key_start = self.find_earliest_write_ru(key, key_map, key_start, gaps_map) # have to start after prev. op
                    else:
                        key_start = self.find_earliest_read_ru(key, key_map, key_start, txn_id, gaps_map)
                if key in key_map:
                    self.insert_key_map(key, key_map, op_type, key_start, key_start, txn_id)
                    # key_map[key].append((op_type, key_start, key_start, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_start, txn_id)]
                self.add_gaps_map(key, key_map, key_start, key_start, op_type, gaps_map)
                prev_key_start = key_start
                txn_end = max(txn_end, key_start)
                last_pos = pos
                # if txn_id == 280: #key_start > 500: #
                #     print(key, key_start, txn_id, key_map[key], gaps_map[key])
            if last_pos != txn_len: # account for residual tail
                txn_end += txn_len - last_pos
            cost = txn_end - total_cost #max_release
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            total_cost += cost
            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            print(gaps_map)
            print(total_cost)

        if self.verify:
            for key, ops in key_map.items():
                key_reads = {}  # <key, set of read indices>
                key_writes = {} # <key, set of write indices>
                for op in ops:
                    (op_type, key_start, key_end, txn_id) = op
                    vals = [x for x in range(key_start, key_end + 1)]
                    for v in vals:
                        if op_type == 'w':
                            if key in key_writes: # check writes don't overlap with writes
                                if v in key_writes[key]:
                                    print(v, key, key_writes[key], op, key_map[key])
                                    print(txn_seq)
                                assert v not in key_writes[key]
                                key_writes[key].append(v)
                            else:
                                key_writes[key] = [v]
                            if key in key_reads: # check writes don't overlap with reads
                                if v in key_reads[key]:
                                    print(v, key, key_reads[key], op, key_map[key])
                                    print(txn_seq)
                                assert v not in key_reads[key]
                                key_reads[key].append(v)
                        else:
                            if key in key_writes: # check that reads don't overlap writes
                                if v in key_writes[key]:
                                    print(v, key, key_writes[key], op, key_map[key])
                                    print(txn_seq)
                                assert v not in key_writes[key]
                            if key in key_reads:
                                key_reads[key].append(v)
                            else:
                                key_reads[key] = [v]

        return total_cost

    def find_max_index_txn_id(self, key, key_map, max_txn_id):
        max_index = 1
        for op in key_map[key]:
            (op_type, key_start, key_end, txn_id) = op
            if txn_id == max_txn_id:
                max_index = max(key_end, max_index)
        return max_index

    def find_other_ops(self, key, key_map, index, op_type):
        other_ops = False
        for op in key_map[key]:
            (typ, key_start, key_end, txn_id) = op
            if op_type == 'w':
                if key_start <= index or key_end >= index:
                    other_ops = True
                    break
            else:
                if key_start <= index or key_end >= index and typ == 'w':
                    other_ops = True
                    break
        return other_ops

    def find_write_si(self, key, key_map, pos, duration, txn_id, max_txn_id):
        if self.debug:
            print(key, key_map[key], pos, duration, txn_id, max_txn_id)
        index = 0
        if max_txn_id != -1:
            index = len(key_map[key]) - 1
            while index >= 0: # find op after max_txn_id
                if key_map[key][index][3] <= max_txn_id:
                    break
                index -= 1
            index += 1
        if self.debug:
            print("write index s: ", index)
        min_index = pos
        if index > 0:
            min_index = max(key_map[key][index-1][2] + 1, min_index)
        running_count = min_index
        if index >= len(key_map[key]):
            running_count = max(self.find_max_index_txn_id(key, key_map, max_txn_id) + 1, running_count)
        else:
            while index < len(key_map[key]): # TODO: same as RC??
                if self.debug:
                    print("curr index: ", index, key_map[key][index])
                if index > 0:
                    if key_map[key][index][1] > running_count + duration and key_map[key][index-1][2] < running_count + duration:
                        break
                else:
                    if key_map[key][index][1] > running_count + duration:
                        break
                if key_map[key][index][3] == txn_id: # as we're adding to key_map
                    running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
                    if not self.find_other_ops(key, key_map, running_count, 'w'):
                        break
                else:
                    running_count = max(key_map[key][index][2] + 1, running_count)
                index += 1

        if self.debug:
            print("running_count: ", running_count)
        return running_count

    def find_write_si_lock(self, key, key_map, pos, duration, txn_end, txn_id, max_txn_id):
        if self.debug:
            print(key, key_map[key], len(key_map[key]), pos, duration, txn_id, max_txn_id)
        index = 0
        if max_txn_id != -1:
            index = len(key_map[key]) - 1
            while index >= 0: # find op after max_txn_id
                if key_map[key][index][3] <= max_txn_id:
                    break
                index -= 1
            index += 1
        if self.debug:
            print("write index s: ", index)
        min_index = pos
        if index > 0:
            min_index = max(key_map[key][index-1][2] + 1, min_index)
        running_count = min_index
        if index >= len(key_map[key]):
            running_count = max(self.find_max_index_txn_id(key, key_map, max_txn_id) + 1, running_count)
        else:
            while index < len(key_map[key]): # TODO: same as RC??
                if index > 0:
                    if key_map[key][index][1] > running_count + duration and key_map[key][index-1][2] < running_count + duration and key_map[key][index][1] > txn_end:
                        break
                else:
                    if key_map[key][index][1] > running_count + duration and key_map[key][index][1] > txn_end:
                        break
                # print("index: ", index, key_map[key][index][3], txn_id, key_map[key][index])
                if key_map[key][index][3] == txn_id: # as we're adding to key_map
                    running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
                    if not self.find_other_ops(key, key_map, running_count, 'w'):
                        break
                else:
                    running_count = max(key_map[key][index][2] + 1, running_count)
                index += 1

        if self.debug:
            print("running_count: ", running_count)
        return running_count

    def find_max_txn_id(self, key, key_map):
        max_txn_id = 0
        index = len(key_map[key]) - 1
        while index >= 0:
            (op_type, key_start, key_end, txn_id) = key_map[key][index]
            if op_type == 'w':
                max_txn_id = txn_id
                break
            index -= 1
        if self.debug:
            print("find max_txn_id: ", max_txn_id, key, key_map)
        return max_txn_id

    def find_read_si(self, key, key_map, pos, txn_id, max_txn_id):
        if self.debug:
            print(key, key_map[key], pos, txn_id, max_txn_id)
        index = 0
        if max_txn_id != -1:
            index = len(key_map[key]) - 1
            while index >= 0: # find op after max_txn_id
                if key_map[key][index][3] <= max_txn_id:
                    break
                index -= 1
            index += 1
        if self.debug:
            print("read index s: ", index)
        min_index = pos
        if index > 0:
            min_index = max(key_map[key][index-1][2] + 1, min_index)
        running_count = min_index
        curr_pos = min_index
        prev_txn_id = -1
        if index >= len(key_map[key]):
            running_count = max(self.find_max_index_txn_id(key, key_map, max_txn_id) + 1, running_count)
        else:
            while index < len(key_map[key]): # TODO: same as RC??
                if index > 0:
                    if curr_pos < key_map[key][index][1] and key_map[key][index][3] != prev_txn_id:
                        if key_map[key][index-1][0] == 'w:':
                            if curr_pos > key_map[key][index-1][2]: # no reads here yet
                                break
                        else:
                            break
                else:
                    if curr_pos < key_map[key][index][1]: # no reads here yet
                        break
                if key_map[key][index][2] >= running_count:
                    if key_map[key][index][0] == 'r':
                        running_count = max(key_map[key][index][1], running_count) # share read lock
                        break
                if key_map[key][index][3] == txn_id: # as we're adding to key_map
                    running_count = max(key_map[key][index][1] + 1, running_count) # same txn so only account for this op
                    if not self.find_other_ops(key, key_map, running_count, 'r'):
                        break
                else:
                    running_count = max(key_map[key][index][2] + 1, running_count)
                if key_map[key][index][2] + 1 > curr_pos:
                    curr_pos = key_map[key][index][2] + 1
                    prev_txn_id = key_map[key][index][3]
                index += 1

        if self.debug:
            print("read running_count: ", running_count)
        return running_count

    # make sure key_map is roughly in order per key
    def insert_key_map(self, key, key_map, op_type, key_start, key_end, txn_id):
        index = len(key_map[key]) - 1
        for op in key_map[key]:
            (_,s,e,_) = key_map[key][index]
            if e <= key_end:
                if s <= key_start:  #e <= key_start or
                    index += 1
                    break
            elif s <= key_start:
                index += 1
                break
            index -= 1
        if index == -1:
            (_,s,e,_) = key_map[key][0]
            if key_end < e and key_start < s:
                key_map[key].insert(0, (op_type, key_start, key_end, txn_id))
            else:
                key_map[key].append((op_type, key_start, key_end, txn_id))
        else:
            key_map[key].insert(index, (op_type, key_start, key_end, txn_id))
        if self.debug:
            print("insert: ", index, key, key_start, key_end, key_map[key])


    def get_si_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
        key_map = {} # <key, [(r/w, lock_start, lock_end)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        txn_id = 0
        for i in range(len(txn_seq)):
            last_pos = 0
            time = i
            txn = self.txns[txn_seq[i]]
            if self.debug:
                print(txn)
            txn_end = 1
            cost = 0
            writes_before_reads = [] # special case of reads after writes to same txn (read locks cannot be shared)
            wr_ops = [] # indicies of reads after writes
            prev_key_start = 0
            reads = set()
            writes = set()
            for j in range(len(txn)): # find r/w to same keys
                (op_type, key, pos, txn_len) = txn[j]
                if op_type == 'r':
                    reads.add(key)
                else:
                    writes.add(key)
            r_w_keys = list(reads.intersection(writes))
            max_txn_id = -1
            for k in r_w_keys: # find the last txn_id that must be visible in snapshot
                if k in key_map:
                    max_txn_id = max(self.find_max_txn_id(k, key_map), max_txn_id)
            if self.debug:
                print("max_txn_id: ", max_txn_id, r_w_keys, reads, writes)
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start
                # if key_start == 0:
                #     key_start = 1
                if self.debug:
                    print("op: ", key, key_start, prev_key_start, pos)
                if key in key_map:
                    if op_type == 'w':
                        key_start = self.find_write_si_lock(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_end, txn_id, max_txn_id) # have to start after prev. op
                    else:
                        key_start = self.find_read_si(key, key_map, key_start, txn_id, max_txn_id)
                if op_type == 'w':
                    writes_before_reads.append(key)
                elif key in writes_before_reads:
                    wr_ops.append(j)
                prev_key_start = key_start
                txn_end = max(txn_end, key_start)
                last_pos = pos
            added_len = 0
            if last_pos != txn_len: # account for residual tail
                txn_end += txn_len - last_pos
                added_len = txn_len - last_pos

            # txn_end might change as we go through so we need to check if it's changed
            curr_txn_end = 0
            tmp_txn_end = txn_end
            while curr_txn_end != txn_end:
                prev_key_start = 0
                last_pos = 0
                for j in range(len(txn)):
                    (op_type, key, pos, txn_len) = txn[j]
                    key_start = pos - last_pos + prev_key_start
                    if self.debug:
                        print("checking: ", key, key_start, prev_key_start, txn[j])
                    if key in key_map:
                        if op_type == 'w': # txn_end - key_start
                            key_start = self.find_write_si_lock(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_end, txn_id, max_txn_id) # have to start after prev. op
                        else:
                            key_start = self.find_read_si(key, key_map, key_start, txn_id, max_txn_id)
                    prev_key_start = key_start
                    txn_end = max(txn_end, key_start)
                    last_pos = pos
                    if self.debug:
                        print("curr txn_end: ", txn_end, key_start, txn_len, added_len)
                if last_pos != txn_len: # account for residual tail
                    txn_end -= added_len
                    txn_end += txn_len - last_pos
                    added_len = txn_len - last_pos
                if self.debug:
                    print("new txn_end: ", txn_end, last_pos, pos)
                curr_txn_end = tmp_txn_end
                tmp_txn_end = txn_end

            cost = txn_end - total_cost #max_release
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            total_cost += cost
            if self.debug:
                print("txn end: ", txn_end, cost, total_cost)

            # need to use txn_end
            prev_key_start = 0
            last_pos = 0
            # write_yet = set() # write for key occured yet
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start
                if self.debug:
                    print("adding: ", key, key_start, prev_key_start, txn[j])
                if j in wr_ops: # if a read is after a write, txn still holds write lock so pretend read is a w
                    op_type = 'w'
                if key in key_map:
                    if self.debug:
                        print("final op: ", txn[j], txn_id)
                    if op_type == 'w':
                        key_start = self.find_write_si_lock(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_end, txn_id, max_txn_id) # have to start after prev. op
                    else:
                        key_start = self.find_read_si(key, key_map, key_start, txn_id, max_txn_id)
                prev_key_start = key_start
                last_pos = pos
                key_end = key_start
                if op_type == 'w':
                    key_end = txn_end

                if self.verify:
                    if key_start > txn_end:
                        print(key, key_start, txn_end, key_map[key])
                        assert False
                if key in key_map:
                    self.insert_key_map(key, key_map, op_type, key_start, key_end, txn_id)
                    # key_map[key].append((op_type, key_start, key_end, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_end, txn_id)]

            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            # print(gaps_map)
            print(total_cost)

        if self.verify:
            for key, ops in key_map.items():
                key_reads = {}
                key_writes = {} # <key, list of (txn_id, write indices)>
                for op in ops:
                    (op_type, key_start, key_end, tid) = op
                    vals = [x for x in range(key_start, key_end + 1)]
                    for v in vals:
                        if op_type == 'w':
                            if key in key_writes: # check writes don't overlap with writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                # if v in key_writes[key] and idv != tid:
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                # if idv != tid:
                                    # assert v not in key_writes[key]
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                            if key in key_reads: # check writes don't overlap with reads
                                matches = [idx for i, (idx, pos) in enumerate(key_reads[key]) if pos == v]
                                # if v in key_reads[key] and idv != tid:
                                #     print(v, key, key_reads[key], op, key_map[key])
                                # if idv != tid:
                                #     assert v not in key_reads[key]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_reads[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                key_reads[key].append((tid, v))
                        else:
                            if key in key_writes: # check that reads don't overlap writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                # if v in key_writes[key] and idv != tid:
                                #     print(v, key, key_writes[key], op, key_map[key])
                                #     print(txn_seq)
                                # if idv != tid:
                                #     assert v not in key_writes[key]
                            if key in key_reads:
                                key_reads[key].append((tid, v))
                            else:
                                key_reads[key] = [(tid, v)]
                    for v in vals: # add write locks after checking
                        if op_type == 'w':
                            if key in key_writes:
                                key_writes[key].append((tid, v))
                            else:
                                key_writes[key] = [(tid, v)]

        return total_cost

    def get_si_opt_seq_cost(self, txn_seq):
        if self.debug:
            print("seq: ", txn_seq)
        key_map = {} # <key, [(r/w, lock_start, lock_end)]>
        prev_txn = txn_seq[0]
        total_cost = 0
        txn_id = 0
        for i in range(len(txn_seq)):
            last_pos = 0
            time = i
            txn = self.txns[txn_seq[i]]

            if self.debug:
                print(txn)
            txn_end = 1
            cost = 0
            prev_key_start = 0
            reads = set()
            writes = set()
            for j in range(len(txn)): # find r/w to same keys
                (op_type, key, pos, txn_len) = txn[j]
                if op_type == 'r':
                    reads.add(key)
                else:
                    writes.add(key)
            r_w_keys = list(reads.intersection(writes))
            max_txn_id = -1
            for k in r_w_keys: # find the last txn_id that must be visible in snapshot
                if k in key_map:
                    max_txn_id = max(self.find_max_txn_id(k, key_map), max_txn_id)
            if self.debug:
                print("max_txn_id: ", max_txn_id, r_w_keys, reads, writes)
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start
                # if key_start == 0:
                #     key_start = 1
                if self.debug:
                    print("op: ", key, key_start, prev_key_start, pos)
                if key in key_map:
                    if op_type == 'w':
                        key_start = self.find_write_si(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_id, max_txn_id) # have to start after prev. op #txn_len - pos + 1
                    else:
                        key_start = self.find_read_si(key, key_map, key_start, txn_id, max_txn_id)
                prev_key_start = key_start
                txn_end = max(txn_end, key_start)
                last_pos = pos
            added_len = 0
            if last_pos != txn_len: # account for residual tail
                txn_end += txn_len - last_pos
                added_len = txn_len - last_pos

            # txn_end might change as we go through so we need to check if it's changed
            curr_txn_end = 0
            tmp_txn_end = txn_end
            while curr_txn_end != txn_end:
                prev_key_start = 0
                last_pos = 0
                for j in range(len(txn)):
                    (op_type, key, pos, txn_len) = txn[j]
                    key_start = pos - last_pos + prev_key_start
                    if self.debug:
                        print("checking: ", key, key_start, prev_key_start, txn[j])
                    if key in key_map:
                        if op_type == 'w': # txn_end - key_start
                            key_start = self.find_write_si(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_id, max_txn_id) # have to start after prev. op
                        else:
                            key_start = self.find_read_si(key, key_map, key_start, txn_id, max_txn_id)
                    prev_key_start = key_start
                    txn_end = max(txn_end, key_start)
                    last_pos = pos
                    if self.debug:
                        print("curr txn_end: ", txn_end, key_start, txn_len, added_len)
                if last_pos != txn_len: # account for residual tail
                    txn_end -= added_len
                    txn_end += txn_len - last_pos
                    added_len = txn_len - last_pos
                if self.debug:
                    print("new txn_end: ", txn_end, last_pos, pos)
                curr_txn_end = tmp_txn_end
                tmp_txn_end = txn_end

            cost = txn_end - total_cost #max_release
            if txn_end <= total_cost: # in some cases, later txn in seq can finish first
                cost = 0
            total_cost += cost
            if self.debug:
                print("txn end: ", txn_end, cost, total_cost)

            # need to use txn_end
            prev_key_start = 0
            last_pos = 0
            # write_yet = set() # write for key occured yet
            for j in range(len(txn)):
                (op_type, key, pos, txn_len) = txn[j]
                key_start = pos - last_pos + prev_key_start
                if self.debug:
                    print("adding: ", key, key_start, prev_key_start, txn[j])
                if key in key_map:
                    if op_type == 'w':
                        key_start = self.find_write_si(key, key_map, key_start, max(txn_end - key_start, txn_len - pos + 1), txn_id, max_txn_id) # have to start after prev. op
                    else:
                        key_start = self.find_read_si(key, key_map, key_start, txn_id, max_txn_id)
                prev_key_start = key_start
                last_pos = pos
                key_end = key_start
                # if op_type == 'w':
                #     key_end = txn_end

                if self.verify:
                    if key_start > txn_end:
                        print(key, key_start, txn_end, txn_id, i)
                        # print(txn_seq)
                        assert False

                if key in key_map:
                    self.insert_key_map(key, key_map, op_type, key_start, key_end, txn_id)
                    # key_map[key].append((op_type, key_start, key_end, txn_id))
                else:
                    key_map[key] = [(op_type, key_start, key_end, txn_id)]

            if self.debug:
                print(key_map)
            txn_id += 1
        if self.debug:
            # print(gaps_map)
            print(total_cost)

        if self.verify:
            for key, ops in key_map.items():
                key_reads = {}
                key_writes = {} # <key, list of (txn_id, write indices)>
                for op in ops:
                    (op_type, key_start, key_end, tid) = op
                    vals = [x for x in range(key_start, key_end + 1)]
                    for v in vals:
                        if op_type == 'w':
                            if key in key_writes: # check writes don't overlap with writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                # if v in key_writes[key] and idv != tid:
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                # if idv != tid:
                                    # assert v not in key_writes[key]
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                            if key in key_reads: # check writes don't overlap with reads
                                matches = [idx for i, (idx, pos) in enumerate(key_reads[key]) if pos == v]
                                # if v in key_reads[key] and idv != tid:
                                #     print(v, key, key_reads[key], op, key_map[key])
                                # if idv != tid:
                                #     assert v not in key_reads[key]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_reads[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                key_reads[key].append((tid, v))
                        else:
                            if key in key_writes: # check that reads don't overlap writes
                                matches = [idx for i, (idx, pos) in enumerate(key_writes[key]) if pos == v]
                                if not (len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)):
                                    print(txn_seq)
                                    print(v, key, tid, matches, key_writes[key], op, key_map[key])
                                assert len(set(matches)) == 0 or (len(set(matches)) == 1 and tid in matches)
                                # if v in key_writes[key] and idv != tid:
                                #     print(v, key, key_writes[key], op, key_map[key])
                                #     print(txn_seq)
                                # if idv != tid:
                                #     assert v not in key_writes[key]
                            if key in key_reads:
                                key_reads[key].append((tid, v))
                            else:
                                key_reads[key] = [(tid, v)]
                    for v in vals: # add write locks after checking
                        if op_type == 'w':
                            if key in key_writes:
                                key_writes[key].append((tid, v))
                            else:
                                key_writes[key] = [(tid, v)]

        return total_cost

    def get_cost(self, txn_seq, cost_type):
        cost = 0
        if cost_type == "opt":
            cost = self.get_opt_seq_cost(txn_seq)
        elif cost_type == "occ":
            cost = self.get_occ_seq_cost(txn_seq)
        elif cost_type == "ru":
            cost = self.get_ru_seq_cost(txn_seq)
        elif cost_type == "rc":
            cost = self.get_rc_seq_cost(txn_seq)
        elif cost_type == "si":
            cost = self.get_si_opt_seq_cost(txn_seq)
        elif cost_type == "sil":
            cost = self.get_si_seq_cost(txn_seq)
        else:
            cost = self.get_seq_cost(txn_seq)
        return cost

    def get_iterated_greedy_cost_sampled(self, num_samples, sample_rate, destruct_size, cost_type):
         # greedy with random starting point
        start_txn = random.randint(0, self.num_txns - 1)
        txn_seq = [start_txn]
        remaining_txns = [x for x in range(0, self.num_txns)]
        remaining_txns.remove(start_txn)
        running_cost = self.txns[start_txn][0][3]
        for i in range(0, self.num_txns - 1):
            min_cost = 100000 # MAX
            min_relative_cost = 10
            min_txn = -1
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
                cost = self.get_cost(test_seq, cost_type)
                # relative_cost = (cost - running_cost * 1.0) / txn_len
                # print(t, relative_cost, cost, running_cost, txn_len)
                if cost < min_cost:
                # if relative_cost < min_relative_cost:
                    min_cost = cost
                    min_txn = t
                    # min_relative_cost = relative_cost
                if done:
                    break
            assert(min_txn != -1)
            running_cost = min_cost
            txn_seq.append(min_txn)
            holdout_txns.remove(min_txn)
            remaining_txns.extend(holdout_txns)
            if self.debug:
                print("min: ", min_txn, remaining_txns, holdout_txns, txn_seq)

        # destruction phase: remove some txns and try again
        destruct_indices = random.sample(range(self.num_txns), destruct_size)
        new_txn_seq = [x for x in txn_seq if x not in destruct_indices]
        if self.debug:
            print(destruct_indices, new_txn_seq)
        txn_seq = new_txn_seq
        running_cost = self.get_cost(new_txn_seq, cost_type)
        remaining_txns = destruct_indices
        for i in range(0, len(destruct_indices)):
            min_cost = 100000 # MAX
            min_relative_cost = 10
            min_txn = -1
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
                cost = self.get_cost(test_seq, cost_type)
                # relative_cost = (cost - running_cost * 1.0) / txn_len
                # print(t, relative_cost, cost, running_cost, txn_len)
                if cost < min_cost:
                # if relative_cost < min_relative_cost:
                    min_cost = cost
                    min_txn = t
                    # min_relative_cost = relative_cost
                if done:
                    break
            assert(min_txn != -1)
            running_cost = min_cost
            txn_seq.append(min_txn)
            holdout_txns.remove(min_txn)
            remaining_txns.extend(holdout_txns)

        if self.debug:
            print(txn_seq)
            print(len(set(txn_seq)))
        assert len(set(txn_seq)) == self.num_txns
        # print(txn_seq)

        overall_cost = self.get_cost(txn_seq, cost_type)
        return overall_cost, txn_seq

    def get_greedy_cost_sampled(self, num_samples, sample_rate, cost_type):
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
                if cost_type == "opt":
                    cost = self.get_opt_seq_cost(test_seq)
                elif cost_type == "occ":
                    cost = self.get_occ_seq_cost(test_seq)
                elif cost_type == "ru":
                    cost = self.get_ru_seq_cost(test_seq)
                elif cost_type == "rc":
                    cost = self.get_rc_seq_cost(test_seq)
                elif cost_type == "si":
                    cost = self.get_si_opt_seq_cost(test_seq)
                elif cost_type == "sil":
                    cost = self.get_si_seq_cost(test_seq)
                else:
                    cost = self.get_seq_cost(test_seq)

                # if opt_cost:
                #     cost = self.get_opt_seq_cost(test_seq, 0)
                # else:
                #     cost = self.get_seq_cost(test_seq, 0)
                    # print(self.get_seq_cost(test_seq, 0))
                    # s_key_map, s_total_cost = self.get_incremental_seq_cost(t, copy.deepcopy(key_map), total_cost)
                    # key_maps.append((s_key_map, s_total_cost))
                    # cost = s_total_cost
                # relative_cost = (cost - running_cost * 1.0) / txn_len
                # print(t, relative_cost, cost, running_cost, txn_len)
                if cost < min_cost:
                # if relative_cost < min_relative_cost:
                    min_cost = cost
                    min_txn = t
                    # min_relative_cost = relative_cost
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
        assert len(set(txn_seq)) == self.num_txns
        # print(txn_seq)

        overall_cost = 0
        if cost_type == "opt":
            overall_cost = self.get_opt_seq_cost(txn_seq)
        elif cost_type == "occ":
            overall_cost = self.get_occ_seq_cost(txn_seq)
        elif cost_type == "ru":
            overall_cost = self.get_ru_seq_cost(txn_seq)
        elif cost_type == "rc":
            overall_cost = self.get_rc_seq_cost(txn_seq)
        elif cost_type == "si":
            overall_cost = self.get_si_opt_seq_cost(txn_seq)
        elif cost_type == "sil":
            overall_cost = self.get_si_seq_cost(txn_seq)
        else:
            overall_cost = self.get_seq_cost(txn_seq)
        # if opt_cost:
        #     overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        # else:
        #     overall_cost = self.get_seq_cost(txn_seq, 0)

        # print(txn_seq, self.get_seq_cost(txn_seq, 0))
        # print("seq:", self.get_random_seq_cost(txn_seq, False, True))
        # print("greedy cost sampled: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost_subseq_sampled(self, num_samples, sample_rate, subseq_len, cost_type):
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
            min_txns = []
            # min_txn = -1
            # min_index = 0
            # holdout_txns = []
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
                ids = []
                if len(remaining_txns) == 0:
                    break
                elif len(remaining_txns) <= subseq_len:
                    ids = remaining_txns
                else:
                    ids = random.sample(remaining_txns, subseq_len)
                random.shuffle(ids)
                # idx = 0
                # if len(remaining_txns) > 1:
                #     idx = random.randint(0, len(remaining_txns) - 1)
                # else:
                #     done = True
                # t = remaining_txns[idx]
                # holdout_txns.append(remaining_txns.pop(idx))
                if self.debug:
                    # print(remaining_txns, holdout_txns)
                    print(ids)
                # txn_len = self.txns[t][0][3]
                test_seq = txn_seq.copy()
                test_seq.extend(ids)
                cost = 0
                if cost_type == "opt":
                    cost = self.get_opt_seq_cost(test_seq)
                elif cost_type == "occ":
                    cost = self.get_occ_seq_cost(test_seq)
                elif cost_type == "ru":
                    cost = self.get_ru_seq_cost(test_seq)
                elif cost_type == "rc":
                    cost = self.get_rc_seq_cost(test_seq)
                elif cost_type == "si":
                    cost = self.get_si_opt_seq_cost(test_seq)
                elif cost_type == "sil":
                    cost = self.get_si_seq_cost(test_seq)
                else:
                    cost = self.get_seq_cost(test_seq)

                # if opt_cost:
                #     cost = self.get_opt_seq_cost(test_seq, 0)
                # else:
                #     cost = self.get_seq_cost(test_seq, 0)
                    # print(self.get_seq_cost(test_seq, 0))
                    # s_key_map, s_total_cost = self.get_incremental_seq_cost(t, copy.deepcopy(key_map), total_cost)
                    # key_maps.append((s_key_map, s_total_cost))
                    # cost = s_total_cost
                # relative_cost = (cost - running_cost * 1.0) / txn_len
                # print(t, relative_cost, cost, running_cost, txn_len)
                if cost < min_cost:
                # if relative_cost < min_relative_cost:
                    min_cost = cost
                    min_txns = ids.copy()
                    # min_relative_cost = relative_cost
                    # min_index = j
                if done:
                    break
                # print(min_txns, ids, test_seq, remaining_txns, cost, min_cost)
            # assert(len(min_txns) != 0)
            running_cost = min_cost
            txn_seq.extend(min_txns)
            for d in min_txns:
                remaining_txns.remove(d)
            # holdout_txns.remove(min_txn)
            # remaining_txns.extend(holdout_txns)
            # key_map, total_cost = key_maps[min_index]
            # if self.debug:
            #     print(txn_seq, key_map, total_cost)

            if self.debug:
                print("min: ", min_txn, txn_seq)
        if self.debug:
            print(txn_seq)
            print(len(set(txn_seq)))
        assert len(set(txn_seq)) == self.num_txns
        # print(txn_seq)

        overall_cost = 0
        if cost_type == "opt":
            overall_cost = self.get_opt_seq_cost(txn_seq)
        elif cost_type == "occ":
            overall_cost = self.get_occ_seq_cost(txn_seq)
        elif cost_type == "ru":
            overall_cost = self.get_ru_seq_cost(txn_seq)
        elif cost_type == "rc":
            overall_cost = self.get_rc_seq_cost(txn_seq)
        elif cost_type == "si":
            overall_cost = self.get_si_opt_seq_cost(txn_seq)
        elif cost_type == "sil":
            overall_cost = self.get_si_seq_cost(txn_seq)
        else:
            overall_cost = self.get_seq_cost(txn_seq)
        # if opt_cost:
        #     overall_cost = self.get_opt_seq_cost(txn_seq, 0)
        # else:
        #     overall_cost = self.get_seq_cost(txn_seq, 0)

        # print(txn_seq, self.get_seq_cost(txn_seq, 0))
        # print("seq:", self.get_random_seq_cost(txn_seq, False, True))
        # print("greedy cost sampled: ", overall_cost)
        return overall_cost, txn_seq

    def get_iterated_greedy_cost_opp_sampled(self, num_samples, sample_rate, destruct_size, cost_type):
        txn_seq = []
        indicies = random.sample(range(self.num_txns), self.num_txns) #[x for x in range(self.num_txns)]#
        running_cost = 0
        for i in range(self.num_txns):
            inserted = False
            min_costs = []
            insert_indicies = []
            txn_indicies = []
            # for k in range(num_samples):
            #     rand_idx = random.randint(0, len(indicies) - 1)
            #     t = indicies[rand_idx]
            #     txn_indicies.append(rand_idx)
            t = indicies.pop(0)
            if len(txn_seq) == 0:
                txn_seq.append(t)
                # indicies.pop(rand_idx)
                # inserted = True
                continue

            sample = random.random()
            if sample > sample_rate:
                txn_seq.append(t)
                # indicies.pop(rand_idx)
                # inserted = True
                continue

            min_cost = 100000 # MAX
            min_relative_cost = 10
            insert_index = -1
            idx = txn_seq
            if len(txn_seq) > num_samples:
                idx = random.sample(range(len(txn_seq)), num_samples)
            for j in idx:
                test_seq = txn_seq.copy()
                test_seq.insert(j, t)
                cost = self.get_cost(test_seq, cost_type)
                if cost < min_cost:
                    min_cost = cost
                    insert_index = j
            #     min_costs.append(min_cost)
            #     insert_indicies.append(insert_index)
            # # running_cost = self.txns[t][0][3]
            # if not inserted:
            #     min_min_cost = min(min_costs)
            #     min_idx = min_costs.index(min_min_cost)
            #     t = indicies.pop(txn_indicies[min_idx])
            #     txn_seq.insert(insert_indicies[min_idx], t)
            #     # print(min_costs, txn_indicies, insert_indicies)
            txn_seq.insert(insert_index, t)

        # destruction phase: remove some txns and try again
        destruct_indices = random.sample(range(self.num_txns), destruct_size)
        new_txn_seq = [x for x in txn_seq if x not in destruct_indices]
        if self.debug:
            print(destruct_indices, new_txn_seq)
        txn_seq = new_txn_seq
        indicies = destruct_indices
        running_cost = self.get_cost(new_txn_seq, cost_type)
        for i in range(0, len(destruct_indices)):
            inserted = False
            min_costs = []
            insert_indicies = []
            txn_indicies = []
            # for k in range(num_samples):
            #     rand_idx = random.randint(0, len(indicies) - 1)
            #     t = indicies[rand_idx]
            #     txn_indicies.append(rand_idx)
            t = indicies.pop(0)
            if len(txn_seq) == 0:
                txn_seq.append(t)
                # indicies.pop(rand_idx)
                # inserted = True
                continue

            sample = random.random()
            if sample > sample_rate:
                txn_seq.append(t)
                # indicies.pop(rand_idx)
                # inserted = True
                continue

            min_cost = 100000 # MAX
            min_relative_cost = 10
            insert_index = -1
            idx = txn_seq
            if len(txn_seq) > num_samples:
                idx = random.sample(range(len(txn_seq)), num_samples)
            for j in idx:
                test_seq = txn_seq.copy()
                test_seq.insert(j, t)
                cost = self.get_cost(test_seq, cost_type)
                if cost < min_cost:
                    min_cost = cost
                    insert_index = j
            #     min_costs.append(min_cost)
            #     insert_indicies.append(insert_index)
            # # running_cost = self.txns[t][0][3]
            # if not inserted:
            #     min_min_cost = min(min_costs)
            #     min_idx = min_costs.index(min_min_cost)
            #     t = indicies.pop(txn_indicies[min_idx])
            #     txn_seq.insert(insert_indicies[min_idx], t)
            #     # print(min_costs, txn_indicies, insert_indicies)
            txn_seq.insert(insert_index, t)

        if self.debug:
            print(txn_seq)
        assert len(set(txn_seq)) == self.num_txns
        # print(txn_seq)

        overall_cost = self.get_cost(txn_seq, cost_type)
        # print("greedy cost opp: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost_opp_sampled(self, num_samples, sample_rate, cost_type):
        txn_seq = []
        indicies = random.sample(range(self.num_txns), self.num_txns) #[x for x in range(self.num_txns)]#
        # print(indicies) [2, 4, 0, 3, 1]#
        running_cost = 0
        for i in range(self.num_txns):
            inserted = False
            min_costs = []
            insert_indicies = []
            txn_indicies = []
            # for k in range(num_samples):
            #     rand_idx = random.randint(0, len(indicies) - 1)
            #     t = indicies[rand_idx]
            #     txn_indicies.append(rand_idx)
            t = indicies.pop(0)
            # print(t, txn_seq)
            if len(txn_seq) == 0:
                txn_seq.append(t)
                # indicies.pop(rand_idx)
                # inserted = True
                continue

            sample = random.random()
            if sample > sample_rate:
                txn_seq.append(t)
                # indicies.pop(rand_idx)
                # inserted = True
                continue

            min_cost = 100000 # MAX
            min_relative_cost = 10
            insert_index = -1
            idx = [x for x in range(len(txn_seq)+1)]
            if len(txn_seq) > num_samples:
                idx = random.sample(range(len(txn_seq)+1), num_samples)
            # print(t, idx)
            for j in idx:
                test_seq = txn_seq.copy()
                test_seq.insert(j, t)
                cost = 0
                if cost_type == "opt":
                    cost = self.get_opt_seq_cost(test_seq)
                elif cost_type == "occ":
                    cost = self.get_occ_seq_cost(test_seq)
                elif cost_type == "ru":
                    cost = self.get_ru_seq_cost(test_seq)
                elif cost_type == "rc":
                    cost = self.get_rc_seq_cost(test_seq)
                elif cost_type == "si":
                    cost = self.get_si_opt_seq_cost(test_seq)
                elif cost_type == "sil":
                    cost = self.get_si_seq_cost(test_seq)
                else:
                    cost = self.get_seq_cost(test_seq)
                if cost < min_cost:
                    min_cost = cost
                    insert_index = j
            txn_seq.insert(insert_index, t)
        if self.debug:
            print(txn_seq)
        assert len(set(txn_seq)) == self.num_txns
        # print(txn_seq)

        overall_cost = 0
        if cost_type == "opt":
            overall_cost = self.get_opt_seq_cost(txn_seq)
        elif cost_type == "occ":
            overall_cost = self.get_occ_seq_cost(txn_seq)
        elif cost_type == "ru":
            overall_cost = self.get_ru_seq_cost(txn_seq)
        elif cost_type == "rc":
            overall_cost = self.get_rc_seq_cost(txn_seq)
        elif cost_type == "si":
            overall_cost = self.get_si_opt_seq_cost(txn_seq)
        elif cost_type == "sil":
            overall_cost = self.get_si_seq_cost(txn_seq)
        else:
            overall_cost = self.get_seq_cost(txn_seq)
        # print("greedy cost opp: ", overall_cost)
        return overall_cost, txn_seq

    def get_greedy_cost_subseq_distr_sampled(self, num_samples, sample_rate, subseq_lens, cost_type):
        for sl in subseq_lens:
            min_txn_seq = []
            min_cost = 100000 # MAX
            cost_map = {}
            for i in range(num_samples):
                cost, txn_seq = self.get_greedy_cost_subseq_sampled(num_samples, sample_rate, sl, cost_type) #get_greedy_cost_sampled(num_txn_samples, sample_rate, opt_cost)
                if cost not in cost_map:
                    cost_map[cost] = 1
                else:
                    cost_map[cost] += 1
                if cost < min_cost:
                    min_cost = cost
                    min_txn_seq = txn_seq
                # if i % 50 == 0:
                #     print("sample: ", i, " min_cost: ", min_cost)
            print("subseq_len: ", sl, " greedy cost min: ", min_cost)

            od = collections.OrderedDict(sorted(cost_map.items()))
            print(od.keys())
            print(od.values())

    def print_stats(self, x, y):
        xv = x
        yv = y
        data = []
        for k, v in zip(xv,yv):
            for i in range(v):
                data.append(k)
        print("count:", len(data))
        print("min: ", min(data))
        print("avg: ", np.array(data).mean())
        print("max: ", max(data))
        print("variance: ", np.var(data))

    def get_greedy_cost_distr_sampled(self, num_samples, num_txn_samples, sample_rate, destruct_size, cost_type):
        min_txn_seq = []
        min_cost = 100000 # MAX
        cost_map = {}
        for i in range(num_samples):
            cost, txn_seq = self.get_greedy_cost_opp_sampled(num_txn_samples, sample_rate, cost_type) #get_greedy_cost_sampled(num_txn_samples, sample_rate, opt_cost)
            # print(cost, txn_seq)
            if cost not in cost_map:
                cost_map[cost] = 1
            else:
                cost_map[cost] += 1
            if cost < min_cost:
                min_cost = cost
                min_txn_seq = txn_seq
            # if i % 50 == 0:
            #     print("sample: ", i, " min_cost: ", min_cost)
        print("greedy opp cost min: ", min_cost)
        print(min_txn_seq)

        od = collections.OrderedDict(sorted(cost_map.items()))
        print(od.keys())
        print(od.values())
        self.print_stats(od.keys(), od.values())

        # min_cost = 100000 # MAX
        # cost_map = {}
        # for i in range(num_samples):
        #     cost, txn_seq = self.get_iterated_greedy_cost_opp_sampled(num_txn_samples, sample_rate, destruct_size, cost_type) #get_greedy_cost_sampled(num_txn_samples, sample_rate, opt_cost)
        #     if cost not in cost_map:
        #         cost_map[cost] = 1
        #     else:
        #         cost_map[cost] += 1
        #     if cost < min_cost:
        #         min_cost = cost
        #         min_txn_seq = txn_seq
        #     # if i % 50 == 0:
        #     #     print("sample: ", i, " min_cost: ", min_cost)
        # print("greedy opp destruct cost min: ", min_cost)

        # od = collections.OrderedDict(sorted(cost_map.items()))
        # print(od.keys())
        # print(od.values())

        # min_cost = 100000 # MAX
        # cost_map = {}
        # for i in range(num_samples):
        #     cost, txn_seq = self.get_iterated_greedy_cost_sampled(num_txn_samples, sample_rate, destruct_size, cost_type) #get_greedy_cost_opp_sampled(num_txn_samples, sample_rate, opt_cost) #
        #     if cost not in cost_map:
        #         cost_map[cost] = 1
        #     else:
        #         cost_map[cost] += 1
        #     if cost < min_cost:
        #         min_cost = cost
        #         min_txn_seq = txn_seq
        #     # if i % 50 == 0:
        #     #     print("sample: ", i, " min_cost: ", min_cost)
        # print("greedy destruct cost min: ", min_cost)

        # od = collections.OrderedDict(sorted(cost_map.items()))
        # print(od.keys())
        # print(od.values())

        min_cost = 100000 # MAX
        cost_map = {}
        for i in range(num_samples):
            cost, txn_seq = self.get_greedy_cost_sampled(num_txn_samples, sample_rate, cost_type) #get_greedy_cost_opp_sampled(num_txn_samples, sample_rate, opt_cost) #
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
        print(min_txn_seq)

        od = collections.OrderedDict(sorted(cost_map.items()))
        print(od.keys())
        print(od.values())
        self.print_stats(od.keys(), od.values())

        return min_txn_seq

    def get_rand_workload_seq(self, txn_seq, cost_type):
        overall_cost = 0
        if cost_type == "opt":
            overall_cost = self.get_opt_seq_cost(txn_seq)
        elif cost_type == "occ":
            overall_cost = self.get_occ_seq_cost(txn_seq)
        elif cost_type == "ru":
            overall_cost = self.get_ru_seq_cost(txn_seq)
        elif cost_type == "rc":
            overall_cost = self.get_rc_seq_cost(txn_seq)
        elif cost_type == "si":
            overall_cost = self.get_si_opt_seq_cost(txn_seq)
        elif cost_type == "sil":
            overall_cost = self.get_si_seq_cost(txn_seq)
        else:
            overall_cost = self.get_seq_cost(txn_seq)
        # print("rand seq cost: ", overall_cost)
        return overall_cost

    def get_random_costs(self, num_seqs, cost_type):
        sequences = []
        costs = []
        for i in range(num_seqs):
            seq = list(np.random.permutation(len(self.txns)))
            sequences.append(seq)
            if cost_type == "opt":
                costs.append(self.get_opt_seq_cost(seq))
            elif cost_type == "occ":
                costs.append(self.get_occ_seq_cost(seq))
            elif cost_type == "ru":
                costs.append(self.get_ru_seq_cost(seq))
            elif cost_type == "rc":
                costs.append(self.get_rc_seq_cost(seq))
            elif cost_type == "si":
                costs.append(self.get_si_opt_seq_cost(seq))
            elif cost_type == "sil":
                costs.append(self.get_si_seq_cost(seq))
            else:
                costs.append(self.get_seq_cost(seq))
            # if i % 100 == 0:
            #     print("seq: ", i)

        min_len = min(costs)
        min_index = list(costs).index(min_len)

        tot_cost = 0.0
        for i, j in zip(sequences, costs):
            # print(sequences[i], j)
            tot_cost += j
        # print("min seq: ", sequences[min_index], "cost: ", min_len)
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

        self.print_stats(od.keys(), od.values())

        return min_len

# r-x ... w-z AND r-z ... w-x
a_key_range = 5
a_read_percent = 0.5
def gen_alternating_workloadr(num_txns):
    workload_str = "{"
    cluster_str = "{"
    for i in range(num_txns):
        workload_str += '"txn' + str(i) + '"' + ":"
        num_ops = 10#random.randint(20,25)#10#random.randint(5,5)#5#
        ops_str = '"'
        first = False
        second = False
        hot_key_1 = str(random.randint(1,a_key_range))
        hot_key_2 = str(random.randint(a_key_range+1,a_key_range*2))
        # hot_key_3 = str(random.randint(a_key_range*2+1,a_key_range*3))
        if i < num_txns / 2: #random.random() < a_read_percent: #
            first = True
            # ops_str += 'r-' + hot_key_1 + " "
            ops_str += 'r-' + hot_key_1 + " " + 'r-' + hot_key_1 + " " #+ 'r-' + hot_key_2 + " "
        else: #elif i < num_txns * 2 / 3: #
            # second = True
            # ops_str += 'r-' + hot_key_2 + " "
            ops_str += 'r-' + hot_key_2 + " " + 'r-' + hot_key_2 + " " #+ 'r-' + hot_key_1 + " "
        # else:
        #     ops_str += 'r-' + hot_key_3 + " "
        for j in range(num_ops - 3):
            ops_str += "*"
            ops_str += " "
        if first:
            # ops_str += 'w-' + hot_key_2 + '"'
            ops_str += 'w-' + hot_key_2 + ' ' + 'w-' + hot_key_2 + ' ' + 'w-' + hot_key_2 + ' ' + 'w-' + hot_key_2 + '"'
        else: #elif second: #
            # ops_str += 'w-' + hot_key_1 + '"'
            ops_str += 'w-' + hot_key_1 + ' ' + 'w-' + hot_key_1 + ' ' + 'w-' + hot_key_1 + ' ' + 'w-' + hot_key_1 + '"'
            # ops_str += 'w-' + hot_key_3 + '"'
        # else:
        #     ops_str += 'w-' + hot_key_1 + '"'

        # if hot_key_1
        # cluster_str += '"txn' + str(i) + '"' + ":" + '"0"'

        # cluster_str += '"txn' + str(i) + '"' + ":" + '"1"'

        workload_str += ops_str
        if i != num_txns -1:
            workload_str += ", "
            cluster_str += ", "
        else:
            workload_str += "}"
            cluster_str += "}"
    # print(cluster_str)
    return workload_str

# https://github.com/apavlo/py-tpcc/blob/master/pytpcc/util/rand.py
# N-O and P transactions only
def gen_tpcc_workload2(num_txns):
    workload_str = "{"
    for i in range(num_txns):
        workload_str += '"txn' + str(i) + '"' + ":"
        w_id = str(random.randint(1,4))
        d_id = str(random.randint(1,10) + int(w_id) * 10)
        c_id = str(random.randint(1,3000) + 1000)
        if i < num_txns / 2: #random.randint(0,1) < 0.5: # N-O or P
            # w_id = str(1)
            workload_str += '"' + "r-" + w_id + " r-" + d_id + " w-" + d_id + " r-" + c_id + " w-" + c_id + " * * * * * * * * * * * * * * * * * *" + '"' # #+ " * * * * * *"
            # workload_str += '"' + "r-" + w_id + " r-" + w_id + " r-" + d_id + " r-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " * * * * * * * * * * * *" + '"' #
        else:
            # w_id = str(2)
            workload_str += '"' + "r-" + w_id + " w-" + w_id + " r-" + d_id + " w-" + d_id + " r-" + c_id + " w-" + c_id + " *" + '"'
            # workload_str += '"' + "r-" + w_id + " r-" + w_id + " w-" + w_id + " w-" + w_id + " w-" + w_id + " w-" + w_id + " r-" + d_id + " r-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " *" + '"'
            # workload_str += '"' + "r-" + w_id + + " r-" + w_id + " w-" + w_id + " w-" + w_id + " w-" + w_id + " w-" + w_id + " w-" + w_id + " r-" + d_id + " r-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " w-" + d_id + " *" + '"'
        if i != num_txns -1:
            workload_str += ", "
        else:
            workload_str += "}"
    return workload_str


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", help="batch size")
    parser.add_argument("-p", action="store_true", help="print workload")

    args = parser.parse_args()

    start_time = time.time()

    batch = 100
    if len(sys.argv) > 2:
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

    start_time = time.time()

    workload = Workload(workloadW, batch, False, True) #False) #


    # print("all =======================================")
    # workload.get_all_sequences("opt")
    # print("time: ", time.time() - start_time)

    print("opt =======================================")
    workload.get_random_costs(100, "opt")
    print("time: ", time.time() - start_time)

    # print("occ =======================================")
    # workload.get_random_costs(1000, "occ")
    # print("time: ", time.time() - start_time)

    # print("ru =======================================")
    # workload.get_random_costs(100, "ru")
    # print("time: ", time.time() - start_time)

    # print("rc =======================================")
    # workload.get_random_costs(1000, "rc")
    # print("time: ", time.time() - start_time)

    # print("si =======================================")
    # workload.get_random_costs(1000, "si")
    # print("time: ", time.time() - start_time)

    # print("sil =======================================")
    # workload.get_random_costs(1000, "sil")
    # print("time: ", time.time() - start_time)

    # print("lock =======================================")
    # workload.get_random_costs(1000, "lock")
    # print("time: ", time.time() - start_time)

    # print("opt =======================================")
    # workload.get_greedy_cost_subseq_distr_sampled(10, 1.0, [100], "opt")
    # print("time: ", time.time() - start_time)

    # workload.get_greedy_cost_subseq_distr_sampled(10, 1.0, [5,10,20], "opt")
    # print("time: ", time.time() - start_time)

    # workload.get_greedy_cost_subseq_sampled(10, 1.0, 2, "opt")
    # print("time: ", time.time() - start_time)

    print("opt =======================================")
    workload.get_greedy_cost_distr_sampled(5, 5, 1.0, 10, "opt")
    print("time: ", time.time() - start_time)

    # print("occ =======================================")
    # workload.get_greedy_cost_distr_sampled(5, 5, 1.0, 20, "occ")
    # print("time: ", time.time() - start_time)

    # print("lock =======================================")
    # workload.get_greedy_cost_distr_sampled(5, 5, 1.0, 10, "lock")
    # print("time: ", time.time() - start_time)

    # print("opt =======================================")
    # workload.get_greedy_cost_distr_sampled(50, 50, 1.0, 10, "opt")
    # print("time: ", time.time() - start_time)

    # print("opt =======================================")
    # workload.get_greedy_cost_distr_sampled(5, 10, 1.0, 25, "opt")
    # print("time: ", time.time() - start_time)

    # print("opt =======================================")
    # workload.get_greedy_cost_distr_sampled(5, 10, 1.0, 50, "opt")
    # print("time: ", time.time() - start_time)

    # print("ru =======================================")
    # workload.get_greedy_cost_distr_sampled(1, 50, 1.0, "ru")
    # print("time: ", time.time() - start_time)

    # print("rc =======================================")
    # workload.get_greedy_cost_distr_sampled(1, 50, 1.0, "rc")
    # print("time: ", time.time() - start_time)

    # print("si =======================================")
    # workload.get_greedy_cost_distr_sampled(1, 50, 1.0, "si")
    # print("time: ", time.time() - start_time)

    # print("sil =======================================")
    # workload.get_greedy_cost_distr_sampled(1, 50, 1.0, "sil")
    # print("time: ", time.time() - start_time)

    # print("lock =======================================")
    # workload.get_greedy_cost_distr_sampled(1, 10, 1.0, "lock")
    # print("time: ", time.time() - start_time)
