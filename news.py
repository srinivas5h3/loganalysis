#!/usr/bin/env python2.7

import psycopg2

'''first question'''
q1 = "1.What are the most popular three articles of all time?"

'''query for first question'''
query1 = ''' select title,count(*) as num from articles,log where
log.path=CONCAT('/article/',articles.slug) group by articles.title order by
num DESC limit 3; '''

'''second question'''
q2 = "2.Who are the most popular article authors of all time?"

'''query for second quetion'''
query2 = ''' select authors.name, count(*) as views from articles inner join
authors on articles.author = authors.id inner join
log on concat('/article/', articles.slug) = log.path where
log.status like '%200%' group by authors.name order by views desc '''

'''third question'''
q3 = "3.On which days did more than 1% of requests lead to errors?"

'''query for third question'''
query3 = ''' select * from (select date(time),round(100.0*sum(case log.status
when '200 OK'  then 0 else 1 end)/count(log.status),3) as error from log group
by date(time) order by error desc) as subq where error > 1; '''

'''main class creating for connect the database and execute the three queries
and dispalys the executed querie and finally diconnect the database'''


class main:
    '''connect the database'''
    def __init__(p):
            p.db = psycopg2.connect('dbname=news')
            p.cursor = p.db.cursor()

    '''close the database'''
    def dbclose(p):
        p.db.close()

    '''executing the queries'''
    def execution(p, query):
        p.cursor.execute(query)
        return p.cursor.fetchall()

    '''display the executing query'''
    def display(p, query, viv='views'):
        r = p.execution(query)
        for i in range(len(r)):
            print '#', r[i][0], '-->', r[i][1], viv
        print("")

if __name__ == '__main__':
    '''object created'''
    ob = main()
    '''print the first question'''
    print q1, "\n"
    ob.display(query1)
    '''print the second question'''
    print q2, "\n"
    ob.display(query2)
    '''print the third question'''
    print q3, "\n"
    ob.display(query3, '% error')
    ob.dbclose()
