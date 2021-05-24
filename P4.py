from scrapy.spiders import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize

import os
import shutil
import logging
import string
import math

logging.disable(30) # disable unneeded logging

doc_lengths = [] # doc length tracking


# -- CRAWLER --
class ConcSpider(CrawlSpider):
    '''Spider class for scraping the concordia site.'''
    name = 'concspider'
    start_urls = ['https://www.concordia.ca']
    allowed_domains = ['www.concordia.ca'] # stay in www.concordia.ca
    custom_settings = {'ROBOTSTXT_OBEY': True, # obey robots.txt
                       'LOG_LEVEL': 'CRITICAL'} # ignore crawling errors
    # ignore french pages and problematic pages
    denies = [r'\/fr\/',
              r'\.ca\/research\/lifestyle-addiction\/tools\/scientific-monitoring\.html\?']
    rules = (Rule(LinkExtractor(deny=denies), 
                  callback="parse_page", 
                  follow=True),)
    i = 0
    def parse_page(self, response):
        '''Parsing function that does the page downloading.'''
        filename = 'crawled/' + str(self.i)
        with open(filename, 'w', encoding='utf-8') as F:
            F.write(response.text)
        with open('IDtoLink.csv', 'a', encoding='utf-8') as F:
            F.write(str(self.i) + ',' + response.url + '\n')
        self.i = self.i + 1
        
def crawl(limit=0):
    '''Initiates webpage crawling. Pages are saved in directory 'crawled'. 
       ID to link mappings saved in 'IDtoLink.csv'.
       'limit' is the number of pages to crawl where 0 is no limit.'''
    open('IDtoLink.csv', 'w')
    if os.path.exists('crawled'):
        shutil.rmtree('crawled')
    os.mkdir('crawled')
    process = CrawlerProcess(settings={'CLOSESPIDER_PAGECOUNT': limit}) # limit retrievals
    process.crawl(ConcSpider)
    process.start()


# -- SCRAPER --
def scrape():
    '''Scrapes the web page texts and yields text-ID tuples of each.'''
    filenames = os.listdir('crawled')
    filenames.sort(key=int) # sort to ensure ID filename ordering
    for filename in filenames: 
        with open('crawled/'+filename, 'r', encoding='utf-8') as F:       
            yield (BeautifulSoup(F.read(), 'lxml').getText(), int(filename))


# -- INDEXER --
def index():
    '''Builds an index from the crawled page texts.'''
    index_block_build(index_tokenize_pair_build(scrape()))
    index_block_merge()
    
def index_tokenize_pair_build(text_id_tuples):
    '''Creates token-ID tuples for each provided page text-ID tuple and yields them.
       Strips punctuation. Applies case folding.'''
    punc = []
    punc[:] = string.punctuation
    punc.extend(["''","``","--","...","..","–","—","‘","’","“","”","•","‹","›","«","·","»"]) 
    for text_id_tuple in text_id_tuples:
        tokens = word_tokenize(text_id_tuple[0])
        doc_lengths.append(len(tokens))
        for token in tokens:
            if token not in punc:
                yield (token.lower(), text_id_tuple[1])

def index_block_build(token_id_tuples, K=1000000):
    '''Builds block dictionaries from K token-ID tuples.
       Writes them to file using index_dict_convert.'''
    if os.path.exists('blocks'):
        shutil.rmtree('blocks')
    os.mkdir('blocks')
    filename = "blocks/Block0"
    d = {}
    for i, token_id_tuple in enumerate(token_id_tuples, 1):
        if (i % K) == 0:
            index_dict_convert(d, filename)
            filename = "blocks/Block" + str(int(i/K))
            d = {}
        if token_id_tuple[0] not in d:
            d[token_id_tuple[0]] = []
        d[token_id_tuple[0]].append(token_id_tuple[1])
    index_dict_convert(d, filename)
    
def index_dict_convert(d, filename):
    '''Writes a block dictionary to file as a sorted index.'''
    with open(filename, 'w', encoding='utf-8') as F:
        for term in sorted(d):
            F.write(term+' '+(",".join(str(id) for id in d[term]))+'\n')

def index_block_merge():
    '''Merges index blocks into one index. Removes the duplicates of the postings lists.
       Appends document frequencies and term frequencies to the index.'''
    blocks = []
    current_lines = []
    for filename in sorted(os.listdir('blocks'), key=lambda x:int(x[5:])):
        blocks.append(open('blocks/'+filename, 'r', encoding='utf-8'))
    for block in blocks:
        current_lines.append(block.readline())
    
    with open('index', 'w', encoding='utf-8') as F:
        while len(blocks) != 0:
            # get block indeces of the smallest term from the current block lines
            smallest_term = None
            smallest_indeces = []
            for i, line in enumerate(current_lines):
                line_term = line.split(' ')[0]
                if smallest_term == None or line_term < smallest_term:
                    smallest_term = line_term
                    smallest_indeces = [i]
                elif line_term == smallest_term:
                    smallest_indeces.append(i)   
            
            # use docIDs of postings lists of smallest to get tfs and sort them
            idtfs = []
            current_id = ''
            current_tf = 0
            for i in smallest_indeces:
                for id in current_lines[i].split(' ')[1].rstrip().split(','):
                    if id == current_id:
                        current_tf = current_tf + 1
                    elif current_id == '':
                        current_id = id
                        current_tf = 1
                    else:
                        idtfs.append((current_id, current_tf))
                        current_id = id
                        current_tf = 1
            idtfs.append((current_id, current_tf))
            idtfs.sort(key = lambda idtf:idtf[1], reverse=True)
            
            # add term, df and top tfs of smallest to file
            F.write(smallest_term+' '+str(len(idtfs))+' ')
            tfsstr = ''
            for id, tf in idtfs[:50]:
                tfsstr = tfsstr + id + ':' + str(tf) + ','
            F.write(tfsstr[:-1])
            F.write('\n')
            
            # read next line on relevant blocks and close any blocks that have reached end of file
            for i in reversed(smallest_indeces):
                current_lines[i] = blocks[i].readline()
                if(current_lines[i] == ''):
                    blocks[i].close()
                    del blocks[i]
                    del current_lines[i]


# -- QUERYER --
def query(q, mode='tfidf', top=15, k1=1.5, b=0.75):
    '''Given a q string, parses out the terms and performs a search on the index.
       Returns the top 'top' results by ranking metric 'mode', either 'tfidf' (default) or 'bm25',
       as a list of ordered link-score pairs where the highest scores are first.
       'k1' and 'b' are constants for use in bm25 only.'''
    query_terms = sorted(list(dict.fromkeys(q.lower().split(' '))))
    i = 0
    doc_scores = {}
    doc_count = len(doc_lengths)
    avrg_doc_length = sum(doc_lengths) / doc_count
    with open('index', 'r', encoding='utf=8') as F:
        done = False
        for line in F:           
            if not done and line.split(' ')[0] == query_terms[i]:
                df = int(line.split(' ')[1])
                for idtf in line.split(' ')[2].split(','):
                    id, tf = map(int, idtf.split(':'))
                    if id not in doc_scores:
                        doc_scores[id] = {}
                    if mode == 'bm25':
                        # apply BM25 to get score
                        doc_scores[id][query_terms[i]] = math.log(doc_count/df)*(((k1 + 1)*tf)/(k1*(1-b) + b*(doc_lengths[id]/avrg_doc_length) + tf))
                    else:                    
                        # apply tfidf to get score
                        doc_scores[id][query_terms[i]] = (1 + math.log(tf)) * math.log(doc_count/df)
                i = i + 1
                if i == len(query_terms):
                    done = True
            if not done and line.split(' ')[0] > query_terms[i]:
                i = i + 1
                if i == len(query_terms):
                    done = True
    results_ranked = []
    for id, doc_score in doc_scores.items():
        results_ranked.append((id, sum(doc_score.values())))
    results_ranked.sort(reverse=True, key=lambda x:x[1])
    results_ranked = results_ranked[:top]
    results_links = []
    i = 0
    with open('IDtoLink.csv', 'r', encoding='utf-8') as F:
        idlinks = F.readlines()
        for result in results_ranked:
            link = idlinks[result[0]].split(',')[1].rstrip()
            results_links.append((link, result[1]))
    return results_links

def query_test(q):
    '''Used to easily provide output for the returns.txt file'''
    qstr = 'query: \'' + q + '\'\n'
    qstr += '\tBM25\n'
    results = query(q, 'bm25')
    if results == []:
        qstr += '\t\tnone\n'
    else:
        for result in results:
            qstr += '\t\t%7.3f %s\n' % (result[1], result[0])
    qstr += '\tTF-IDF\n'
    results = query(q)
    if results == []:
        qstr += '\t\tnone\n'
    else:
        for result in results:
            qstr += '\t\t%7.3f %s\n' % (result[1], result[0])
    return qstr


# -- MAIN --
# crawling
crawl_flag = True
if os.path.exists('crawled') and os.path.isfile('IDtoLink.csv'):
    print('crawl files already present')
    inp = ''
    while inp not in ['y','n','Y','N']:
        inp = input('would you like to redo crawling? (y/n):')
    if inp in ['n','N']:
        crawl_flag = False
if crawl_flag:
    inp = ''
    while not inp.isnumeric() or int(inp) < 0:
        inp = input('crawl limit (0 for none): ')
    print('crawling www.concordia.ca...')
    crawl(int(inp))
    print('crawling finished\n')
else:
    print('skipping crawling\n')
 
# indexing
index_flag = True
if not crawl_flag and os.path.isfile('index') and os.path.isfile('doclengths'):
    print('index files already present')
    inp = ''
    while inp not in ['y','n','Y','N']:
        inp = input('would you like to redo indexing? (y/n):')
    if inp in ['n','N']:
        index_flag = False    
if index_flag:
    print('building index from crawled pages...')
    index()
    print('index built')
    with open('doclengths', 'w') as F:
        F.write(','.join(map(str, doc_lengths)))
    print('doc lengths written to file\n')
else:
    print('skipping indexing')
    with open('doclengths', 'r') as F:
        doc_lengths = list(map(int, F.readline().split(',')))
    print('doc lengths loaded from file\n')
    
# queries
query_flag = True
if os.path.isfile('returns.txt'):
    print('query returns file already present')
    inp = ''
    while inp not in ['y','n','Y','N']:
        inp = input('would you like to re run queries? (y/n):')
    if inp in ['n','N']:
        query_flag = False 
if query_flag:
    print('running queries on the built index...')
    q1s = ['researcher covid-19',
           'researcher coronavirus']
    q2s = ['faculty environment sustainability energy water',
           'department environment sustainability energy water']
    qcs = ['water management sustainability Concordia',
           'Concordia COVID-19 faculty',
           'SARS-CoV Concordia faculty']
    breakstr = '-'*120
    with open('returns.txt', 'w', encoding='utf-8') as F:
        F.write(breakstr+'\n')
        F.write('Which researchers at Concordia worked on COVID 19-related research?')
        F.write('\n'+breakstr+'\n')
        for q in q1s:
            F.write(query_test(q)+'\n')
        F.write('\n'+breakstr+'\n')
        F.write('Which departments at Concordia have research in environmental issues, sustainability, energy and water conservation?')
        F.write('\n'+breakstr+'\n')
        for q in q2s:
            F.write(query_test(q)+'\n')
        F.write('\n'+breakstr+'\n')
        F.write('Challenge Queries')
        F.write('\n'+breakstr+'\n')
        for q in qcs:
            F.write(query_test(q)+'\n')
    print('query results saved to returns.txt\n')
else:
    print('skipping querying\n')


input('press enter to exit')