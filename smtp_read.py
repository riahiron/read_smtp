import smtplib
import time
import imaplib
import email
import pymongo
from pymongo import MongoClient


# -------------------------------------------------
#
# Utility to read email from Gmail Using Python
#
# ----------------------------------


ORG_EMAIL   = "@gmail.com"
FROM_EMAIL  = "ronytest61" + ORG_EMAIL
FROM_PWD    = "$$$$$$$$"
#SMTP_SERVER = "imap.gmail.com"

SMTP_SERVER = "pop.gmail.com"
SMTP_PORT   = 993


#SERVER = "pop.gmail.com"
#USER  = "ronytest61@gmail.com"
#PASSWORD = "rony_test_61"

def read_email_from_gmail():
    try:
        mail = imaplib.IMAP4_SSL(SMTP_SERVER)
        mail.login(FROM_EMAIL,FROM_PWD)
        mail.select('inbox')
        EmailDict={}
        ReturnDict=[]

        type, data = mail.search(None, 'ALL')
        mail_ids = data[0]

        id_list = mail_ids.split()   
        first_email_id = int(id_list[0])
        latest_email_id = int(id_list[-1])
        print str(latest_email_id)


        for i in range(latest_email_id,first_email_id, -1):
            typ, data = mail.fetch(i, '(RFC822)' )

            for response_part in data:
                if isinstance(response_part, tuple):

                    msg = email.message_from_string(response_part[1])
                    email_subject = msg['subject']
                    email_from = msg['from']
                    print 'From : ' + email_from + '\n'
                    print 'Subject : ' + email_subject + '\n'
                    
                    #message read

                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            ctype = part.get_content_type()
                            cdispo = str(part.get('Content-Disposition'))

                            # skip any text/plain (txt) attachments
                            if ctype == 'text/plain' and 'attachment' not in cdispo:
                                body = part.get_payload(decode=True)  # decode
                                break
                    # not multipart - i.e. plain text, no attachments, keeping fingers crossed
                    else:
                        body = b.get_payload(decode=True)
                        
                    

                    for i in ( 'email_subject', 'email_from','body'):
                        EmailDict[i] = locals()[i]
                    ReturnDict.append(EmailDict)


        mail.close()
        return ReturnDict


    except Exception, e:
        print str(e)

def removeNonAscii(s):
    if ord(s) < 48 or ord(s) > 127: return ''


def word_count(string):
    my_string = string.lower().split()
    #f_string=filter(removeNonAscii,my_string)
    my_dict = {}

    for item in my_string:
        if item in my_dict:
            my_dict[item] += 1
        else:
            my_dict[item] = 1
    return my_dict



def connect_to_mongod(_input_):

    #Atals connect sample
    #client = pymongo.MongoClient("mongodb://kay:myRealPassword@mycluster0-shard-00-00.mongodb.net:27017,mycluster0-shard-00-01.mongodb.net:27017,mycluster0-shard-00-02.mongodb.net:27017/admin?ssl=true&replicaSet=Mycluster0-shard-0&authSource=admin")
    #db = client.test


    #db = client.test
    cluster="cluster0-shard-00-00-i24ll.mongodb.net:27017,cluster0-shard-00-01-i24ll.mongodb.net:27017,cluster0-shard-00-02-i24ll.mongodb.net"
    mongodbstr ="mongodb://user_name:password@"+cluster+"/MongoDB?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin"
    #mongodbstr ="mongodb://riahiron:Neta_Amir2018@mycluster0-shard-00-00.mongodb.net:27017,mycluster0-shard-00-01.mongodb.net:27017,mycluster0-shard-00-02.mongodb.net:27017/admin?ssl=true&replicaSet=Mycluster0-shard-0&authSource=admin"

    try:
        client = pymongo.MongoClient(mongodbstr)
        db = client.MongoDB
        
        posts = db.mail_data
        
        #print(client.server_info())
        post_id = posts.insert_one(_input_)

    except Exception as e:
        print(e)
    else:
        pass
    finally:
        pass

if __name__ == "__main__":
    dictval=read_email_from_gmail()
    
    for key, value in dictval[0].iteritems() :
        print key

    i=0
    for x in dictval:
        str=dictval[i].get('body',None)
        listofword=word_count(str)
        #print(listofword)


        filtered_list = dict(filter(lambda x: x[1] >2, listofword.items()))
        print(filtered_list)

        dictval[i]['key_words']=filtered_list
        connect_to_mongod(dictval[i])
        #connect_to_mongod(listofword)
        i=+1
    
    
