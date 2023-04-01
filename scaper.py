import time
import requests
from bs4 import BeautifulSoup

g=open("neoxa_block_log.csv",'r')
gLines=g.readlines()
gNumberLine=len(gLines)
g.close()

f = open('neoxa_block_log.csv', 'a', newline='')

if gNumberLine==0:
    f.write("block number, timestamp, hash, size, weight, version, merkleroot, tx, number transaction, difficulty, chainwork, headerhash, mixhash, pow winner, block reward, output amount, fee amount\n") #write header
    last_block_found=0
elif gNumberLine==1:
    last_block_found=0
elif gNumberLine>1:
    last_block_found=int(gLines[-1].split(",")[0])
    print("From data, last block: "+str(last_block_found))
g.close()

last_blockchain_block=int(requests.get("https://explorer.neoxa.net/api/getblockcount").text)
print("Last blockchain block: "+str(last_blockchain_block))

startTime=time.time()
print("Start at "+str(startTime))
i=last_block_found+1
while i<last_blockchain_block:
    data=""

    try:
        response1=requests.get("https://explorer.neoxa.net/api/getblockhash?index="+str(i)).text
        #delay here and not in except ?
        response=requests.get("https://explorer.neoxa.net/api/getblock?hash="+str(response1))
    except:
        time.sleep(4)
        print("explorer neoxa timeout")
        continue

    try:
        response=response.json()
    except:
        print(response.text)

    data=str(i)+','+str(response["time"])+','+str(response["hash"])+','+str(response["size"])+','+str(response["weight"])+','+str(response["version"])+','+str(response["merkleroot"])+','+str(response["tx"]).replace(",",";")+','+str(len(response["tx"]))+','+str(response["difficulty"])+','+str(response["chainwork"])+','+str(response["headerhash"])+','+str(response["mixhash"])
    
    try:
        response=requests.get("https://neoxa.cryptoscope.io/block/block.php?blockheight="+str(i)).text
    except:
        time.sleep(1)
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

    i+=1



finishTime=time.time()
print("Finish at "+str(finishTime))
print("The time spent on: "+str(finishTime-startTime))
# close the file
f.close()




