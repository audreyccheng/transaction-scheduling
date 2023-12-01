from itertools import combinations, permutations
from scipy import stats
from scipy.stats import zipf
from scipy.cluster.vq import whiten
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.decomposition import PCA

from parse_cc import Workload

import argparse
import collections
import copy
import json
import math
import numpy as np
import random
import sys
import time

POPULATION_SIZE = 100

class Search(Workload):
    def __init__(self, workload_json, cutoff, debug=False, verify=False):
        super().__init__(workload_json, cutoff, debug=False, verify=False)

    def get_random_seq(self):
        return list(np.random.permutation(len(self.txns)))

    # neighborhood function is a swap move, i.e., replace the order of two jobs
    def get_neighbor(self, seq):
        txn_seq = copy.deepcopy(seq)
        first_index = random.randint(0, len(txn_seq) - 1)
        second_index = random.randint(0, len(txn_seq) - 1)
        while first_index == second_index:
            second_index = random.randint(0, len(txn_seq) - 1)

        temp_val = txn_seq[first_index]
        txn_seq[first_index] = txn_seq[second_index]
        txn_seq[second_index] = temp_val

        return txn_seq

    def simulated_annealing(self, cost_type, starting_seq):
        """Peforms simulated annealing to find a solution"""
        initial_temp = 90
        final_temp = .1
        alpha = 0.01

        current_temp = initial_temp

        # Start by initializing the current state with the initial state
        current_state = starting_seq
        current_cost = self.get_rand_workload_seq(current_state, cost_type)
        solution = current_state

        count = 0
        while current_temp > final_temp:
            neighbor = self.get_neighbor(current_state)
            neighbor_cost = self.get_rand_workload_seq(neighbor, cost_type)

            # Check if neighbor is best so far
            cost_diff = current_cost - neighbor_cost

            # if the new solution is better, accept it
            if cost_diff >= 0:
                solution = neighbor
                current_cost = neighbor_cost
            # if the new solution is not better, accept it with a probability of e^(-cost/temp)
            else:
                val = float('inf')
                try:
                    val = math.exp(-cost_diff / current_temp)
                except OverflowError:
                    val = float('inf')
                if random.uniform(0, 1) < val:
                    solution = neighbor
                    current_cost - neighbor_cost
            # decrement the temperature
            current_temp -= alpha
            count += 1

        print("simulated_annealing num iterations: ", count, " min: ", current_cost)

        return solution

    def mate(self, seq1, seq2):
        '''
        Perform mating and produce new offspring
        '''

        # chromosome for offspring
        all_nums = set(seq1)
        remaining_txns = set()
        child_chromosome = []

        # prob = random.random()
        # cutoff = (int) (len(seq1) / 2)
        # if (prob < 0.50):
        #     child_chromosome.extend(seq1[:cutoff])
        #     child_chromosome.extend(seq2[cutoff+1:])
        # else:
        #     child_chromosome.extend(seq2[:cutoff])
        #     child_chromosome.extend(seq1[cutoff+1:])

        for gp1, gp2 in zip(seq1, seq2):
            # random probability
            prob = random.random()
            chosen = False

            # if prob is less than 0.45, insert gene
            # from parent 1
            if prob < 0.45 and not gp1 in remaining_txns:
                child_chromosome.append(gp1)
                remaining_txns.add(gp1)
                all_nums.remove(gp1)
                chosen = True

            # if prob is between 0.45 and 0.90, insert
            # gene from parent 2
            elif prob < 0.90 and not gp2 in remaining_txns:
                child_chromosome.append(gp2)
                remaining_txns.add(gp2)
                all_nums.remove(gp2)
                chosen = True

            # otherwise insert random gene(mutate),
            # for maintaining diversity
            if not chosen:
                rand_num = all_nums.pop()
                child_chromosome.append(rand_num)
                remaining_txns.add(rand_num)

        assert(len(set(child_chromosome)) == len(seq1))

        # create new Individual(offspring) using
        # generated chromosome for offspring
        return child_chromosome

    def genetic_algorithm(self, cost_type, starting_seq, starting_cost, total_gen):
        global POPULATION_SIZE

        #current generation
        generation = 1

        found = False
        population = {} # <cost, seq>

        # create initial population
        for _ in range(POPULATION_SIZE):
            seq = self.get_random_seq()
            cost = self.get_rand_workload_seq(seq, cost_type)
            population[cost] = seq

        population[starting_cost] = starting_seq

        while generation < total_gen:
            # print(population.keys())
            # print(population.values())

            od = collections.OrderedDict(sorted(population.items()))

            # Otherwise generate new offsprings for new generation
            new_generation = {}

            # Perform Elitism, that mean 10% of fittest population
            # goes to the next generation
            count = 0
            s = int((10*POPULATION_SIZE)/100)
            for k, v in od.items():
                if count > s:
                    break;
                count += 1
                new_generation[k] = v

            # From 50% of fittest population, Individuals
            # will mate to produce offspring
            count = 0
            s = int((90*POPULATION_SIZE)/100)
            cutoff = (int) (POPULATION_SIZE / 2)
            keys = list(od.keys())[:cutoff]
            for _ in range(s):
                parent1 = random.randint(0, len(keys) - 1)
                parent2 = random.randint(0, len(keys) - 1)
                child = self.mate(od[keys[parent1]], od[keys[parent2]])
                cost = self.get_rand_workload_seq(child, cost_type)
                new_generation[cost] = child

            population = new_generation

            generation += 1

        fod = collections.OrderedDict(sorted(population.items()))

        assert(len(set(list(od.values())[0])) == len(starting_seq))
        print("genetic_algorithm num generations: ", generation, " min: ", list(fod.keys())[0]) #, " cost: ", self.get_rand_workload_seq(list(od.values())[0], cost_type))
        # print("seq: ", list(od.values())[0])

    def get_tabu_neighbors(self, solution):
        # TODO: Implement your neighborhood function here
        # The neighborhood function should generate
        # a set of neighboring solutions based on a given solution
        # Example: Generate neighboring solutions by
        # swapping two elements in the solution

        neighbors = []
        for i in range(len(solution)):
            for j in range(i + 1, len(solution)):
                neighbor = solution[:]
                neighbor[i], neighbor[j] = neighbor[j], neighbor[i]
                neighbors.append(neighbor)
        return neighbors

    def tabu_search(self, initial_solution, max_iterations, tabu_list_size, cost_type):
        best_solution = initial_solution
        current_solution = initial_solution
        tabu_list = []

        count = 0
        for _ in range(max_iterations):
            neighbors = self.get_tabu_neighbors(current_solution)
            best_neighbor = None
            best_neighbor_fitness = float('inf')

            for neighbor in neighbors:
                if neighbor not in tabu_list:
                    neighbor_fitness = self.get_rand_workload_seq(neighbor, cost_type)
                    if neighbor_fitness < best_neighbor_fitness:
                        best_neighbor = neighbor
                        best_neighbor_fitness = neighbor_fitness

            if best_neighbor is None:

                # No non-tabu neighbors found,
                # terminate the search
                break

            current_solution = best_neighbor
            tabu_list.append(best_neighbor)
            if len(tabu_list) > tabu_list_size:

                # Remove the oldest entry from the
                # tabu list if it exceeds the size
                tabu_list.pop(0)

            if self.get_rand_workload_seq(best_neighbor, cost_type) < self.get_rand_workload_seq(best_solution, cost_type):
                # Update the best solution if the
                # current neighbor is better
                best_solution = best_neighbor

            count += 1

        assert(len(best_solution) == len(initial_solution))
        print("tabu_search iterations: ", count, " min: ", self.get_rand_workload_seq(best_solution, cost_type))
        # return best_solution

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

    search = Search(workloadW, batch, False, True)

    type = "opt"

    search.get_random_costs(1000, type)
    print("time: ", time.time() - start_time, "\n")

    # greedy_seq = search.get_greedy_cost_distr_sampled(5, 5, 1.0, 10, type)
    # print("time: ", time.time() - start_time, "\n")

    search.simulated_annealing(type, search.get_random_seq())
    print("time: ", time.time() - start_time, "\n")

    # search.simulated_annealing(type, greedy_seq)
    # print("time: ", time.time() - start_time, "\n")

    seq = search.get_random_seq()
    search.genetic_algorithm(type, seq, search.get_rand_workload_seq(seq, type), 100)
    print("time: ", time.time() - start_time, "\n")

    # search.genetic_algorithm(type, greedy_seq, search.get_rand_workload_seq(greedy_seq, type), 100)
    # print("time: ", time.time() - start_time, "\n")

    search.tabu_search(search.get_random_seq(), 10, 100, type)
    print("time: ", time.time() - start_time, "\n")

    # search.tabu_search(greedy_seq, 10, 100, type)
    # print("time: ", time.time() - start_time, "\n")

    # workload = Workload(workloadW, batch, False, True)
