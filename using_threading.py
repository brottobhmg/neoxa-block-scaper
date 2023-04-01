import random
import threading
import time
import requests
from bs4 import BeautifulSoup


def task(start, finish):
    f = open('neoxa_block_log_'+str(start)+'.csv', 'a', newline='')
    f.write("block number, timestamp, hash, size, weight, version, merkleroot, tx, number transaction, difficulty, chainwork, headerhash, mixhash, pow winner, block reward, output amount, fee amount\n") #write header

    while start<finish:
        data=""
        try:
            response1=requests.get("https://explorer.neoxa.net/api/getblockhash?index="+str(start)).text
            #delay here and not in except ?
            response=requests.get("https://explorer.neoxa.net/api/getblock?hash="+str(response1))
        except:
            time.sleep(random.randint(1,6))
            print("explorer neoxa timeout")
            continue
        try:
            response=response.json()
        except:
            print(response.text)
            continue
        data=str(start)+','+str(response["time"])+','+str(response["hash"])+','+str(response["size"])+','+str(response["weight"])+','+str(response["version"])+','+str(response["merkleroot"])+','+str(response["tx"]).replace(",",";")+','+str(len(response["tx"]))+','+str(response["difficulty"])+','+str(response["chainwork"])+','+str(response["headerhash"])+','+str(response["mixhash"])
        try:
            response=requests.get("https://neoxa.cryptoscope.io/block/block.php?blockheight="+str(start)).text
        except:
            time.sleep(random.randint(0,2))
            print("cryptoscope .php timeout")
            continue
        soup = BeautifulSoup(response, 'html.parser')
        if i!=0:
            powWinner=soup.find_all('a')[-1].string #powWinner
            block_reward=soup.find_all('td')[17].text.strip() #block reward
            output_amount=soup.find_all('td')[21].text.strip() #output amount
            fee_amount=soup.find_all('td')[-1].text.strip() #fee amount
        else:
            powWinner= soup.find_all('td')[9].text
            block_reward= soup.find_all('td')[13].text
            output_amount=soup.find_all('td')[15].text.strip()
            fee_amount=soup.find_all('td')[-1].text.strip()
        data+=','+str(powWinner)+','+str(block_reward)+','+str(output_amount)+','+str(fee_amount)
        f.write(data+"\n")
        start+=1
        global totalRecordWriten
        totalRecordWriten+=1
    f.close()





nThread=10
last_blockchain_block=460000#int(requests.get("https://explorer.neoxa.net/api/getblockcount").text)
block_interval=[]
totalRecordWriten=0
blockFrom=0 #from first block

for i in range(blockFrom,last_blockchain_block,int(last_blockchain_block/nThread)):
    block_interval.append([i,i+int(last_blockchain_block/nThread)])

print(block_interval)
#[[0, 46598], [46598, 93196], ...]


for i in range(nThread):
    thread = threading.Thread(target=task, args=(block_interval[i][0],block_interval[i][1]))
    print("Created thread "+thread.name)
    thread.start()
    


print("Waiting ...")
while not threading.active_count()==0:
    print(str(totalRecordWriten))
    time.sleep(5*60)
print("All threads finish")

#TODO joining file .csv