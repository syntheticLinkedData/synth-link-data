import lp
import datetime
import conn_str
import psycopg2
import random




CC_q_filename = "CC_queries_all_10_good.txt"
V1_CC_filename = "V1_CC_dict_all_10_good.txt"
output_filename = "details.txt"

lp.main(CC_q_filename, V1_CC_filename, output_filename)




io_stats = open(output_filename, "a")
io_stats.write("\n\n\tPartition people and households by PUMA10-TEN ---- start time %s\n" %(datetime.datetime.now()))
'''------------------------------------------------------------------------------------------------------------------------
                                                h_id UNKNOWN for our problem
------------------------------------------------------------------------------------------------------------------------'''
# CREATE A DICTIONARY THAT PARTITIONS PEOPLE INTO PUMAs ON THE BASIS OF V1
l_ppl_in_diff_puma_ten = {}
try:
    connect_str = conn_str.get_conn_str()
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM V1")
    rows = cursor.fetchall()
    for i in range(len(rows)):
        p_id = rows[i][0]
        ten = rows[i][5]
        puma = rows[i][4]
        if (puma, ten) not in l_ppl_in_diff_puma_ten:
            l_ppl_in_diff_puma_ten[(puma, ten)] = [p_id]
        else:
            l_ppl_in_diff_puma_ten[(puma, ten)].append(p_id)

    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)


# CREATE A DICTIONARY THAT PARTITIONS HHs INTO PUMAs ON THE BASIS OF h
l_hhs_in_diff_puma_ten = {}
try:
    connect_str = conn_str.get_conn_str()
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM V2")
    rows = cursor.fetchall()
    for i in range(len(rows)):
        h_id = rows[i][0]
        puma = rows[i][1]                     # idx 1 in V2(h_id, PUMA10, TEN) but 3 in h(h_id, TEN, ST, PUMA10)
        ten = int(rows[i][2])
        if (puma, ten) not in l_hhs_in_diff_puma_ten:
            l_hhs_in_diff_puma_ten[(puma, ten)] = [h_id]
        else:
            l_hhs_in_diff_puma_ten[(puma, ten)].append(h_id)
    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)
io_stats.write("\tPartition people and households by PUMA10-TEN ---- end time %s\n" %(datetime.datetime.now()))
'''---------------------------------------------------------------------------------------------------------------------'''


'''------------------------------------------------------------------------------------------------------------------------
                                        RANDOM h_id ASSIGNMENT FROM CANDIDATE FK VALUES
------------------------------------------------------------------------------------------------------------------------'''
lp.save_time(datetime.datetime.now())

h_new_l = -1
h_new_u = -1

for (puma, ten), l_ppl in l_ppl_in_diff_puma_ten.items():
    candidate_fk = []
    if puma == -1:
        continue
    if (puma, ten) in l_hhs_in_diff_puma_ten:
        candidate_fk = l_hhs_in_diff_puma_ten[(puma, ten)]
    else:
        h_new_l -= len(l_ppl)
        candidate_fk = [i for i in range(h_new_l, h_new_u)]
        h_new_u = h_new_l
    
    io_stats.write("\n\n\n\n---------------------------PUMA10 = %3d - TEN = %1d---------------------------\n" %(puma, ten))
    #io_stats.write("h_id candidates = %s\n" %(candidate_fk))
    io_stats.write("Number of people = %s\n" %(len(l_ppl)))
    
    io_stats.write("\tUpdate p_with_hid ---- start time %s\n" %(datetime.datetime.now()))
    '''------------------------------------------------------------------------------------------------------------------------
                                        Update h_id value in p_with_hid for people in this PUMA10
    ------------------------------------------------------------------------------------------------------------------------'''
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        
        # Assign values to h_id in relation p_with_hid
        for i in l_ppl:
            fk = random.choice(candidate_fk)
            cursor.execute("UPDATE p_with_hid SET h_id=" + str(fk) + " WHERE p_id=" + str(i))
            # If need a new home, add corresponding row to V_2
            cursor.execute("SELECT COUNT(*) FROM V2 WHERE h_id=" + str(fk))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO V2(h_id, PUMA10, TEN) VALUES(" + str(fk) + "," + str(puma) + "," + str(ten) + ")")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
    io_stats.write("\tUpdate p_with_hid ---- end time %s" %(datetime.datetime.now()))
    io_stats.write("\n----------------------------------------------------------------------------\n")
lp.save_time(datetime.datetime.now())








'''------------------------------------------------------------------------------------------------------------------------
                                                    Compute Stats (for coloring)
------------------------------------------------------------------------------------------------------------------------'''
err = 0
try:
    connect_str = conn_str.get_conn_str()
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    
    # CHECK CC VIOLATIONS
    io_stats.write("\n\n\n\nCCs with target counts minus counts in solution\n")
    q = open(CC_q_filename, "r")
    for line in q:
        k_v = line.rstrip().split(":")
        pred = k_v[0]
        target_count = int(k_v[1])

        cursor.execute("SELECT COUNT(*) FROM p_with_hid NATURAL JOIN V2 WHERE " + pred)
        ans = cursor.fetchone()[0]

        io_stats.write("%4d - %4d = %4d \t%s\n" %(target_count, ans, target_count - ans, pred))
        err += abs(target_count - ans)
    q.close()

    # CHECK DC VIOLATIONS
    io_stats.write("\nDCs with number of pairs of (tuple) violations\n")
    q = open("./DC_queries.txt", "r")
    DC_err = 0
    for line in q:
        k_v = line.rstrip().split(":")
        cursor.execute(k_v[0])
        ans = cursor.fetchone()[0]
        DC_err += ans
        io_stats.write(str(ans) + "\t" + k_v[1] + "\n")
    q.close()
    io_stats.write("\n")

    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)




'''---------------------------------------------------------------------------------------------------------------------'''
lp.save_tot_L1_err("%s (DC)\n" %(DC_err))
lp.save_tot_L1_err("%s\n----------------------------------------\n" %(err))
io_stats.write("\n\n\n\n\n")
io_stats.close()
