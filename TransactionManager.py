from graph import Graph
from components import Site, Variable, Lock, Operation, Transaction
from datetime import datetime

class TransactionManager:
    def __init__(self):
        # 10 sites (site index: )
        self.sites = dict() 
        # varSite (variable index: list of site indexes where it's stored)
        self.varSite = dict() 
        # transactions (transaction index: transaction)
        self.transactions = dict()
        # graph for deadlock check
        self.graph = Graph()
        # odd indexed variables are at one site each
        # even indexed variables are at all sites
        for varIndex in range(1, 21):
            self.varSite[varIndex] = list()
            if varIndex % 2 == 0:
                for j in range(1, 11):
                    self.varSite[varIndex].append(j)
            else:
                self.varSite[varIndex].append(varIndex % 10 + 1)
        for siteIndex in range(1, 11):
            self.sites[siteIndex] = Site(siteIndex) # initialize the sites

