#!/usr/bin/env python
# coding: utf-8

# In[5]:


import glob
import tqdm
import numpy as np
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
from itertools import combinations



adilson_papers = pd.read_csv('rank_cit_v3.csv')



adilson_papers_set = set(adilson_papers['doi'])


# In[4]:


path = '/mnt/openalex/slices/subset-1990-2022/dec-2023/'
works_path = 'works'
works_authorships_path = 'works_authorships'
works_referenced_works_path = 'works_referenced_works'


# In[ ]:


works_dd = pd.read_parquet(path + works_path)#, npartitions=10)


# In[7]:


# In[16]:


adilson_works_dd = works_dd[works_dd['doi'].isin(adilson_papers_set)]



def load_dict(filename):
    input_file = open(filename)
    file_dict = dict()
    for line in input_file:
        key, values = line[:-1].split('\t')
        values = values[1:-1].split(',')
        values = [int(v) for v in values]
        file_dict[int(key)] = values
    return file_dict


# In[12]:


index2cited_index = load_dict('../index2cited_index')


# In[13]:


index2citing_index = load_dict('../index2citing_index')


# In[18]:


focal_papers = set(adilson_works_dd.index)# ids dos artigos



index2ID = list(works_dd.index)
ID2index = {id: index for index, id in enumerate(index2ID)}


# In[ ]:


def get_in(index): # ARTIGOS QUE CITARAM INDEX
    if index in index2cited_index:
        return index2cited_index[index]
    return []

def get_out(index): # ARTIGOS REFERENCIADOS POR INDEX / INDEX CITA
    if index in index2citing_index:
        return index2citing_index[index]
    return []
    
Dindex_list = []

for focal_paper in tqdm.tqdm(focal_papers, total=len(focal_papers)):
    focal_index = ID2index[focal_paper]
    if focal_index in index2citing_index: # focal_index precisa ter referencias
        #print(focal_paper)
    
        citing_focal_paper = get_in(focal_index)
        cited_by_focal_paper = set(get_out(focal_index))

        cited_by_citing_focal_paper = [set(get_out(paper_id)) for paper_id in citing_focal_paper]
        cited_by_citing_focal_paper = [cited_by_focal_paper & set(citing_list) for citing_list in cited_by_citing_focal_paper]
        
        citing_only_focal = 0
        citing_focal_and_refs_of_focal = 0
        for test in cited_by_citing_focal_paper:
            if len(test) > 0:
                citing_focal_and_refs_of_focal += 1
            else:
                citing_only_focal += 1        

        citing_refs_of_focal = [get_in(paper_id) for paper_id in cited_by_focal_paper]
        citing_refs_of_focal_set = set()
        for papers in citing_refs_of_focal:
            citing_refs_of_focal_set |= set(papers)
        citing_refs_of_focal_set = list(citing_refs_of_focal_set)
        citing_only_refs_of_focal = 0
        for paper in citing_refs_of_focal_set:
            paper_refs = get_out(paper)
            if focal_index not in paper_refs:
                citing_only_refs_of_focal += 1

        Dindex = (citing_only_focal - citing_focal_and_refs_of_focal)/(citing_only_focal + citing_focal_and_refs_of_focal + citing_only_refs_of_focal)
        # print(Dindex)
        Dindex_list.append((focal_paper, citing_only_focal, citing_focal_and_refs_of_focal, citing_only_refs_of_focal, Dindex))

pd.DataFrame(Dindex_list, columns=['paper_openalex_id', 'only_focal', 'focal_and_refs', 'only_refs', 'Dindex']).to_csv('Dindex_list_adilson')


# In[ ]:




