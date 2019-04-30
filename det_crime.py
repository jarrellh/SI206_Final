import pandas as pd
import string
import csv, sqlite3
from sodapy import Socrata
import sportradar.NFL
import datetime
import matplotlib
import matplotlib.pyplot as plt 

def sportrad():
    # accessing sportradar API
    key = '7q9rc7t53kw9d6dc4jqektzb'
    client = sportradar.NFL.NFL(key)
    sched = client.get_schedule(2017, "REG").json()

    # writing sportradar data to csv
    with open("lions2017.csv", "w") as f_out:
        f_out.write("Location,League,Home Team,Away Team,Outcome,Scheduled Date,Scheduled Time\n")
        for x in range(17):
            for game in sched["weeks"][x]["games"]:
                outcome=0
                if game["home"]["name"] == "Detroit Lions":
                    if game["scoring"]["home_points"]>game["scoring"]["away_points"]:
                        outcome = "home win"
                    else: 
                        outcome = "home loss"
                    scheduleddate = []
                    scheduledtime = []
                    scheduleddate = game["scheduled"][0:10]
                    scheduledtime = game["scheduled"][11:19]
                    f_out.write(game["venue"]["city"]+","+"NFL"+ ","+\
                                game["home"]["name"]+","+game["away"]["name"]+","+ outcome + ","+\
                                scheduleddate+","\
                                +scheduledtime+"\n")

def det_crime_data():
    # accessing detroit police department API
    username = "jarrellh@umich.edu"
    password = "yoit$Jrl97"
    client = Socrata("data.detroitmi.gov", 'o2fuwAx0ibOWLVYQQrAXsJi2G', username='jarrellh@umich.edu', password='yoit$Jrl97')

    # runnign SoQL query to obtain desired data
    results = client.get("6gdg-y3kf", content_type='csv',select='date_trunc_ymd(incident_timestamp) as inc_date,offense_category,count(crime_id) as crime_count', where="incident_timestamp between '2017-09-01T00:00:00.000' and '2018-01-01T00:00:00.000' and neighborhood='Downtown' and (offense_category='STOLEN VEHICLE' or offense_category='ASSAULT' OR offense_category='LARCENY' OR offense_category='ROBBERY')", group='inc_date,offense_category',order='inc_date')
    results_df = pd.DataFrame.from_records(results)
    return results

def combo_sql(results):
    # create sqlite table for lions data
    con = sqlite3.connect('det.sqlite')
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS lions')
    cur.execute("CREATE TABLE lions (outcome TEXT, game_date DATE)")
    # write lions data to sqlite table
    with open('lions2017.csv', 'r') as lions_data:
        reader = csv.DictReader(lions_data)
        for row in reader:
            if row['Outcome'] == 'home win':
                _outcome = True
            else:
                _outcome = False
            _game_date = row['Scheduled Date']
            cur.execute("INSERT INTO lions (outcome, game_date) VALUES (?,?)", (_outcome, _game_date))
    con.commit()

    # create new sqlite table for crime data
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS crime_table')
    cur.execute("CREATE TABLE crime_table (crime_count INTEGER, inc_date DATE, offense_category TEXT)")

    # wrtie results data into sqlite table
    for row in results:
        _crime_count = row[0]
        _inc_date = row[1][:10]
        _offense_category = row[2]
        cur.execute("INSERT INTO crime_table (crime_count, inc_date, offense_category) VALUES (?,?,?)", (_crime_count, _inc_date, _offense_category))
    con.commit()

    # c.inc_date, c.offense_category, c.crime_count, l.outcome
    cur.execute('SELECT * FROM crime_table c LEFT JOIN lions l ON c.inc_date = l.game_date')
    joined_table = cur.fetchall()
    return joined_table

def fin_csv(joined_table):
    # initialize data structures
    dict = {}
    index = 7
    for row in joined_table:
        if row[0] == 'crime_count':
            continue
        # create indices for type of crime
        if row[2] == 'ASSAULT':
            index = 0
        elif row[2] == 'LARCENY':
            index = 1
        elif row[2] == 'STOLEN VEHICLE':
            index = 2
        elif row[2] == 'ROBBERY':
            index = 3
        date = row[1]
        if date not in dict:
            # initialize key, value combo. assault, larceny, stolen vehicle, robbery, has_game, game_outcome
            dict[date] = [0,0,0,0,0,False,None]
            if row[4] is not None:
                dict[date][5] = True
                dict[date][6] = row[3]
        # count of crime for that date
        dict[date][index] = row[0]
        # increment total crime count for that date
        dict[date][4] += row[0]

    csv_list = []
    csv_list.append(['Date','Assault','Larceny','Stolen Vehicle','Robbery','Total_Crimes','Has_Game','Game_Outcome'])
    for key in dict:
        row_list = []
        row_list.append(key)
        row_list.extend(dict[key])
        csv_list.append(row_list)

    with open("final_data.csv", "w") as final:
        writer = csv.writer(final)
        writer.writerows(csv_list)
    return csv_list

def calcs(csv_list):
    c_dict = {}
    # gameday, non game day, win, loss
    c_dict['Assault'] = [0,0,0,0]
    c_dict['Larceny'] = [0,0,0,0]
    c_dict['Stolen Vehicle'] = [0,0,0,0]
    c_dict['Robbery'] = [0,0,0,0]
    game_count = 0
    win_count = 0
    loss_count = 0
    non_game_count = 0
    for row in csv_list:
        if row[6] == 'Has_Game':
            continue
        if row[6] == True:
            c_dict['Assault'][0] += row[1]
            c_dict['Larceny'][0] += row[2]
            c_dict['Stolen Vehicle'][0] += row[3]
            c_dict['Robbery'][0] += row[4]
            if row[7] == '1':
                c_dict['Assault'][2] += row[1] 
                c_dict['Larceny'][2] += row[2]
                c_dict['Stolen Vehicle'][2] += row[3]
                c_dict['Robbery'][2] += row[4]
                win_count +=1
            else:
                c_dict['Assault'][3] += row[1] 
                c_dict['Larceny'][3] += row[2]
                c_dict['Stolen Vehicle'][3] += row[3]
                c_dict['Robbery'][3] += row[4]
                loss_count += 1
            game_count += 1
        else:
            c_dict['Assault'][1] += row[1] 
            c_dict['Larceny'][1] += row[2]
            c_dict['Stolen Vehicle'][1] += row[3]
            c_dict['Robbery'][1] += row[4]
            non_game_count += 1

    for crime in c_dict:
        c_dict[crime][0] = c_dict[crime][0]/game_count
        c_dict[crime][1] = c_dict[crime][1]/non_game_count
        c_dict[crime][2] = c_dict[crime][2]/win_count
        c_dict[crime][3] = c_dict[crime][3]/loss_count
    return c_dict

def chart1(dict):
    #Chart 1: Show compare the frequency of four crimes between gamedays and non-gamedays 
    Names = ['Gameday', 'Non-Gameday']
    AssaultValues = [dict['Assault'][0],dict['Assault'][1]]
    StolenVehicleValues = [dict['Stolen Vehicle'][0],dict['Stolen Vehicle'][1]]
    LarcenyValues = [dict['Larceny'][0],dict['Larceny'][1]]
    RobberyValues = [dict['Robbery'][0],dict['Robbery'][1]]

    fig = plt.figure(figsize=(20,5))
    ax1 = fig.add_subplot(141)
    ax2 = fig.add_subplot(142)
    ax3 = fig.add_subplot(143)
    ax4 = fig.add_subplot(144)

    ax1.bar(Names,AssaultValues)
    ax1.set_xlabel('Assault')
    ax2.bar(Names,StolenVehicleValues)
    ax2.set_xlabel("Stolen Vehicle")
    ax3.bar(Names,LarcenyValues)
    ax3.set_xlabel('Larceny')
    ax4.bar(Names,RobberyValues)
    ax4.set_xlabel('Robbery')

    plt.show()

def chart2(dict):
    #Chart 3: Compare each type of crime on gamedays between wins and losses 
    Names = ['Win', 'Loss']
    AssaultValues = [dict['Assault'][2],dict['Assault'][3]]
    StolenVehicleValues = [dict['Stolen Vehicle'][2],dict['Stolen Vehicle'][3]]
    LarcenyValues = [dict['Larceny'][2],dict['Larceny'][3]]
    RobberyValues = [dict['Robbery'][2],dict['Robbery'][3]]

    fig = plt.figure(figsize=(20,5))
    ax1 = fig.add_subplot(141)
    ax2 = fig.add_subplot(142)
    ax3 = fig.add_subplot(143)
    ax4 = fig.add_subplot(144)

    ax1.bar(Names,AssaultValues)
    ax1.set_xlabel('Assault')
    ax2.bar(Names,StolenVehicleValues)
    ax2.set_xlabel("Stolen Vehicle")
    ax3.bar(Names,LarcenyValues)
    ax3.set_xlabel('Larceny')
    ax4.bar(Names,RobberyValues)
    ax4.set_xlabel('Robbery')

    plt.show()

def main():
    sportrad()
    results = det_crime_data()
    joined_table = combo_sql(results)
    csv_list = fin_csv(joined_table)
    dict = calcs(csv_list)
    chart1(dict)
    chart2(dict)

if __name__ == "__main__":
    main()