"""parser.py is a parser that translate input text to executable info for transaction manager.

Contribution of authors:
    Implemented by Yubing Bai

The details of functions are specified below every definition of them.
"""

import re
import TransactionManager
from absl import flags, app
import time

FLAGS = flags.FLAGS
debugMode = TransactionManager.debugMode

flags.DEFINE_string('filename', None, 'test file directory')
flags.mark_flag_as_required('filename')

def lines():
    """Print a line.
    """
    temp = '- ' * 20
    print(temp)

def extractNum(target):
    """extract number from given string.
    Input:
        target: a given string
    Output:
        the number exists in the string.
    """
    temp = re.findall(r'\d+', target)
    return list(map(int, temp))[0]

def extractContent(line):
    """extract items in the parenthesis in a string.
    Input:
        target: a given string
    Output:
        a list of strings in the parenthesis delimited by comma.
    """
    if debugMode:
        print("start: ", line)
    regex = re.compile(r'[(](.*?)[)]', re.S)
    content = re.findall(regex, line)[0]
    content = content.split(",")
    content = [i.strip() for i in content]
    content = [i for i in content if i]
    return content

def parse_line(line, tx_manager): 
    """Parse the give line and invoke transaction manager to execute.
    Parser is able to read the following lines:
        begin(T1)
        beginRO(T1)
        W(T1, x10, 3)
        R(T1, x3)
        end(T1)
        fail(3): site 3 fails
        recover(3): site 3 recovers
        dump(): dump all sites
        dump(1, 3, 5): dump site 1, 3, and 5
    """   
    if line.startswith('begin('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        tx_manager.startTx('RW', transaction_id)
        time.sleep(.0001)
        
    elif line.startswith('beginRO('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        tx_manager.startTx('RO', transaction_id)
        time.sleep(.01)
        
    elif line.startswith('W('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        variable_id = extractNum(content[1])
        variable_val = int(content[2])
        if transaction_id in tx_manager.transactions:
            tx_manager.writeOp(transaction_id, variable_id, variable_val)
        else:
            if debugMode:
                print('Error: ', line)
                print('T',transaction_id, " do not exists yet.")
    elif line.startswith('R('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        variable_id = extractNum(content[1])
        if transaction_id in tx_manager.transactions:
            tx_manager.readOp(transaction_id, variable_id)
        else:
            if debugMode:
                print('Error: ', line)
                print('T',transaction_id, " do not exists yet.")
    elif line.startswith('end('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        if transaction_id in tx_manager.transactions:
            tx_manager.endTx(transaction_id)
            
    elif line.startswith('recover('):
        content = extractContent(line)
        siteId = int(content[0])
        tx_manager.recoverOp(siteId)
    elif line.startswith('fail('):
        content = extractContent(line)
        siteId = int(content[0])
        tx_manager.failOp(siteId)
    elif line.startswith('dump('):
        content = extractContent(line)
        if len(content) == 0:
            tx_manager.dumpOp()
        else:
            sites = []
            print("content is: ", content)
            for s in content:
                sites.append(int(s))
            tx_manager.dumpOp(sites)

def parse_file(filename):
    """read in given file and parse the whole file.
    Input:
        filename: the directory of test text file.
    """
    tx_manager = TransactionManager.TransactionManager()
    lines()
    print('Start: ', filename)
    lines()
    with open(filename) as fp:
        line =  fp.readline()
        while line:
            parse_line(line.strip(), tx_manager)
            line = fp.readline()
    lines()
    print('Finished.')
    lines()

def main(args):
    if FLAGS.filename:
        parse_file(FLAGS.filename)
    else:
        exit()

if __name__ == '__main__':
    app.run(main)


            