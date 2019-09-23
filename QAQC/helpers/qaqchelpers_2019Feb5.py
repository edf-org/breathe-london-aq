
#convert hexadecimal string to binary string
def hex2bin(hex_str):
    lhex_str = hex_str.lower()
    hb_dict = {'0':'0000','1':'0001','2':'0010','3':'0011','4':'0100','5':'0101','6':'0110'\
,'7':'0111','8':'1000','9':'1001','a':'1010','b':'1011','c':'1100','d':'1101','e':'1110','f':'1111'}
    return ('').join([hb_dict[h] for h in lhex_str])

#retrieve list of bit indices
def bin2bits(bin_str,inverse=False):
    bit_list = []
    if inverse:
        marked ='0'
    else:
        marked ='1'
    for i,s in enumerate(bin_str[::-1]):
        if s==marked:
            bit_list.append(i) 
    return bit_list
