import re
import TransactionManager

def parse_file(filename):
    trans_manager = transactionmanager.TransactionManager()
    print_header()
    print('begin processing for file ', filename)
    print_header()
    with open(filename) as fp:
        line =  fp.readline()
        while line:
            parse_line(line.strip(), trans_manager)
            line = fp.readline()
    print_header()
    print('end processing for file ', filename)
    print_header()

def print_header():
    stars = '- ' * 20
    print(stars)

def extractNum(target):
    temp = re.findall(r'\d+', target)
    return list(map(int, temp))[0]

def extractContent(line):
    regex = re.compile(r'[(](.*?)[)]', re.S)
    content = re.findall(regex, line)[0]
    content = content.split(",")
    content = [i.strip() for i in content]
    return content

def parse_line(line, trans_manager):    
    if line.startswith('begin('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        trans_manager.start_transaction('RW', transaction_id)
        
    elif line.startswith('beginRO('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        trans_manager.start_transaction('RO', transaction_id)
        
    elif line.startswith('W('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        variable_id = extractNum(content[1])
        variable_val = int(content[2])
        if transaction_id in trans_manager.transaction_map:
            trans_manager.insert_site_to_trans_map(trans_num, v_ind)
            trans_manager.write_op(transaction_id, variable_id, variable_val)
        else:
            print('Error: ', line)
            print('transaction_id', transaction_id)
            print('trans_manager.transaction_map', trans_manager.transaction_map)
            print('end')
    elif line.startswith('R('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
        variable_id = extractNum(content[1])
        if transaction_id in trans_manager.transaction_map:
            trans_manager.insert_site_to_trans_map(transaction_id, variable_id)
            trans_manager.read_op(transaction_id, variable_id)
        else:
            print('Error: ', line)
            print('transaction_id', transaction_id)
            print('trans_manager.transaction_map', trans_manager.transaction_map)
            print('end')
    elif line.startswith('end('):
        content = extractContent(line)
        transaction_id = extractNum(content[0])
#         if transaction_id in trans_manager.transaction_map:
#             trans_manager.end_op(transaction_id)
            
#     elif line.startswith('recover('):
#         site_number = find_pure_number(line)
#         # print('recover', site_number)
#         trans_manager.recover_site(site_number)
#     elif line.startswith('fail('):
#         site_number = find_pure_number(line)
#         # print('fail', site_number)
#         trans_manager.fail_site(site_number)
#     elif line.startswith('dump('):
#         if 'x' in line:
#             v_ind = find_variable_ind(line)
#             # print('dump x', v_ind)
#             trans_manager.dump_single_val(v_ind)
#         elif re.match(r'\d+', line):
#             v_val = find_pure_number(line)
#             # print('dump v', v_val)
#             trans_manager.dump_single_site(v_val)
#         else:
#             # print('dump all')
#             trans_manager.dump_all()