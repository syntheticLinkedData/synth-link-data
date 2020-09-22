import psycopg2
from pulp import *
import math
import datetime
import conn_str
import random




def get_intervals(i):
    return attr_intervalization[i]

def get_num_S_CCs():
    return num_S_CC

def set_num_S_CCs(n):
    num_S_CCs = n








def no_agep(relp_val, sex_val, type_of_tuples, n_men):
    n_men_women = len(type_of_tuples)

    n_women = n_men_women-n_men
    row = [0]*n_men_women

    if relp_val == -1 and sex_val == -1:                                            # Use all vars
        row = [1]*n_men_women
        return row
    elif sex_val != -1 and sex_val[0] == 1:                                         # Which men?
        if relp_val == -1:                                                          # No RELP => All men
            row = [1]*n_men
            row.extend([0]*n_women)
        else:                                                                       # Only those with given RELP
            for j in range(n_men):
                if type_of_tuples[j][1] == relp_val[0]:
                    row[j] = 1
        return row
    elif sex_val != -1 and sex_val[0] == 2:                                         # Which women?
        if relp_val == -1:                                                          # No RELP => All women
            row = [0]*n_men
            row.extend([1]*n_women)
        else:                                                                       # Only those with given RELP
            for j in range(n_men, n_men_women):
                if type_of_tuples[j][1] == relp_val[0]:
                    row[j] = 1
        return row
    elif sex_val == -1 and relp_val != -1:                                          # Which relp? (Both men and women)
        for j in range(n_men_women):
            if type_of_tuples[j][1] == relp_val[0]:
                row[j] = 1
        return row


def helper_given_agep_men(agep_val, type_of_tuples, u, row, given_relp, relp_val):
    for j in range(u):
        if type_of_tuples[j][0][0] >= agep_val[0] and type_of_tuples[j][0][1] <= agep_val[1] and \
            (not given_relp or type_of_tuples[j][1] == relp_val[0]):
            row[j] = 1
        elif type_of_tuples[j][0][0] > agep_val[1]:
            break
    return row

def helper_given_agep_women(agep_val, type_of_tuples, l, u, row, given_relp, relp_val):
    for j in range(l, u):
        if type_of_tuples[j][0][0] >= agep_val[0] and type_of_tuples[j][0][1] <= agep_val[1] and \
            (not given_relp or type_of_tuples[j][1] == relp_val[0]):
            row[j] = 1
        elif type_of_tuples[j][0][0] > agep_val[1]:
            break
    return row

def given_agep(agep_val, relp_val, sex_val, type_of_tuples, n_men):
    n_men_women = len(type_of_tuples)

    n_women = n_men_women-n_men
    row = [0]*n_men_women

    if relp_val == -1 and sex_val == -1:                                            # Use all vars with age in specified range
        # Check for both men and women
        row = helper_given_agep_men(agep_val, type_of_tuples, n_men, row, False, relp_val)
        return helper_given_agep_women(agep_val, type_of_tuples, n_men, n_men_women, row, False, relp_val)
    elif sex_val != -1 and sex_val[0] == 1:                                         # Which men?
        if relp_val == -1:
            return helper_given_agep_men(agep_val, type_of_tuples, n_men, row, False, relp_val)
        else:
            return helper_given_agep_men(agep_val, type_of_tuples, n_men, row, True, relp_val)
    elif sex_val != -1 and sex_val[0] == 2:
        if relp_val == -1:
            return helper_given_agep_women(agep_val, type_of_tuples, n_men, n_men_women, row, False, relp_val)
        else:
            return helper_given_agep_women(agep_val, type_of_tuples, n_men, n_men_women, row, True, relp_val)
    elif sex_val == -1 and relp_val != -1:
        row = helper_given_agep_men(agep_val, type_of_tuples, n_men, row, True, relp_val)
        return helper_given_agep_women(agep_val, type_of_tuples, n_men, n_men_women, row, True, relp_val)








def get_existing_tuple_types(R1_name):
    type_of_tuples = []
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        cursor.execute("SELECT AGEP, RELP, SEX FROM " + R1_name + " GROUP BY AGEP, RELP, SEX ORDER BY SEX, AGEP, RELP")
        distinct_tuples = cursor.fetchall()

        for i in distinct_tuples:
            type_of_tuples.append([int(i[0]), int(i[1]), int(i[2])])

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?\n")
        print(e)
    return type_of_tuples








def save_time(t):
    with open("lp_times.txt", "a+") as myfile:
        myfile.write(str(t)+"\n")

def save_num_HHs_ppl(n_hh, n_ppl):
    with open("lp_times.txt", "a+") as myfile:
        myfile.write("#HHs = %s ---- #Ppl = %s\n" %(n_hh, n_ppl))

def save_tot_L1_err(err):
    with open("lp_times.txt", "a+") as myfile:
        myfile.write("Error = %s\n" %(err))








p_table = "p_allpumas_10"
h_table = "h_allpumas_10"

num_R1_non_key_attrs = 3
num_R2_non_key_attrs = 2

attr_intervalization = [[] for i in range(5)]
num_S_CC = 0

dom_ten = [1, 2, 3, 4]
dom_puma = [100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,\
         129,130,131,132,133,134,200,201,202,203,204,205,206,207,208,209,300,301,302,303,304,305,306,307,308,400,401,402,403,\
         404,405,406,407,408,409,410,411,412,413,500,501,502,503,504,505,506,507,508,600,601,602,603,604,605,700,701,702,703,\
         704,705,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818,819,820,821,822,823,824,900,901,\
         902,903,904,905,906,907,908,909,910,1000,1001,1002,1003,1004,1005,1006,1007,1008,1100,1101,1102,1103,1104,1105,1106,\
         1107,1108,1109,1110,1111,1112,1113,1114,1200,1201,1202,1203,1204,1205,1206,1207,1208,1300,1301,1302,1303,1304,1305,\
         1306,1307,1308,1309,1314,1316,1317,1318,1319,1320,1321,1322,1323,1324,1400,1401,1402,1403,1404,1405,1406,1407,1408,\
         1409,1410,1500,1501,1502,1503,1504,1600,1601,1602,1603,1604,1700,1701,1702,1703,1704,1705,1706,1800,1801,1802,1803,\
         1804,1805,1806,1807,1808,1900,1901,1902,1903,1904,1905,1906,1907,2000,2001,2002,2003,2004,2005,2006,2100,2101,2102,\
         2103,2104,2200,2201,2202,2203,2300,2301,2302,2303,2304,2305,2306,2307,2308,2309,2310,2311,2312,2313,2314,2315,2316,\
         2317,2318,2319,2320,2321,2322,2400,2401,2402,2500,2501,2502,2503,2504,2505,2506,2507,2508,2509,2510,2511,2512,2513,\
         2514,2515,2516,2600,2601,2602,2603,2700,2701,2702,2703,2800,2801,2802,2803,2900,2901,2902,2903,2904,2905,2906,2907,\
         2908,3000,3001,3002,3003,3004,3005,3006,3007,3008,3009,3100,3101,3102,3103,3104,3105,3106,3107,3108,3200,3201,3202,\
         3203,3204,3205,3206,3207,3208,3209,3210,3211,3212,3213,3300,3301,3302,3303,3304,3305,3306,3307,3308,3309,3310,3311,\
         3312,3313,3400,3401,3402,3403,3404,3407,3408,3409,3410,3411,3412,3413,3414,3415,3416,3417,3418,3419,3420,3421,3422,\
         3500,3501,3502,3503,3504,3520,3521,3522,3523,3524,3525,3526,3527,3528,3529,3530,3531,3532,3600,3601,3602,3603,3700,\
         3701,3702,3703,3704,3705,3706,3707,3708,3709,3710,3711,3712,3713,3714,3715,3716,3717,3718,3719,3720,3721,3722,3723,\
         3724,3725,3726,3727,3728,3729,3730,3731,3732,3733,3734,3735,3736,3737,3738,3739,3740,3741,3742,3743,3744,3745,3746,\
         3747,3748,3749,3750,3751,3752,3753,3754,3755,3756,3757,3758,3759,3760,3761,3762,3763,3764,3765,3766,3767,3768,3769,\
         3800,3801,3802,3803,3804,3805,3806,3807,3808,3809,3810,3900,3901,3902,3903,4000,4001,4002,4003,4004,4005,4006,4007,\
         4008,4009,4010,4011,4012,4013,4014,4015,4016,4017,4018,4100,4101,4102,4103,4104,4105,4106,4107,4108,4109,4110,4111,\
         4112,4113,4114,4200,4300,4301,4302,4303,4400,4500,4501,4502,4503,4504,4600,4601,4602,4603,4604,4605,4606,4607,4608,\
         4609,4610,4611,4612,4613,4614,4615,4616,4617,4618,4619,4620,4621,4622,4623,4624,4625,4626,4627,4628,4629,4630,4631,\
         4632,4633,4634,4635,4636,4637,4638,4700,4701,4702,4800,4801,4802,4803,4900,4901,4902,4903,4904,4905,5000,5001,5002,\
         5003,5100,5200,5201,5202,5203,5204,5300,5301,5302,5303,5304,5305,5306,5307,5308,5309,5400,5401,5402,5403,5500,5501,\
         5502,5503,5504,5505,5506,5507,5600,5700,5701,5702,5703,5704,5705,5706,5707,5708,5800,5901,5902,5903,5904,5905,5906,\
         5907,5908,5909,5910,5911,5912,5913,5914,5915,5916,5917,5918,6000,6001,6002,6100,6101,6102,6103,6200,6300,6301,6302,\
         6400,6500,6501,6502,6503,6504,6505,6506,6507,6508,6509,6510,6511,6512,6513,6514,6515,6601,6602,6603,6701,6702,6703,\
         6704,6705,6706,6707,6708,6709,6710,6711,6712,6801,6802,6803,6804,6805,6806,6807,6900,6901,6902,6903,7101,7102,7103,\
         7104,7105,7106,7107,7108,7109,7110,7111,7112,7113,7114,7115,7300,7301,7302,7303,7304,7305,7306,7307,7308,7309,7310,\
         7311,7312,7313,7314,7315,7316,7317,7318,7319,7320,7321,7322,7501,7502,7503,7504,7505,7506,7507,7701,7702,7703,7704,\
         7901,7902,8101,8102,8103,8104,8105,8106,8301,8302,8303,8500,8501,8502,8503,8504,8505,8506,8507,8508,8509,8510,8511,\
         8512,8513,8514,8601,8602,8603,8604,8605,8606,8607,8608,8609,8610,8611,8612,8613,8614,8615,8616,8617,8618,8619,8620,\
         8621,8622,8623,8624,8700,8701,8702,8900,9100,9300,9501,9502,9503,9504,9505,9506,9507,9508,9509,9510,9701,9702,9703,\
         9901,9902,9903,9904,9905,9906,9907,9908,9909,9910,9911,10000,10100,10101,10102,10103,10104,10200,10300,10301,10302,\
         10303,10304,10305,10306,10307,10308,10400,10501,10502,10503,10504,10600,10700,10701,10702,10703,10800,10900,10901,\
         10902,11000,11001,11002,11101,11102,11103,11104,11105,11106,11200,11300,11401,11402,11501,11502,11503,11504,11505,\
         11506,11507,11601,11602,11603,11604,11605,11606,11607,11608,11609,11610,11611,11612,11613,11614,11615,11616,11701,\
         11702,11703,11704,11705,11706,11801,11802,11900,12100,12701,12702,12703,12704,13001,20000,21001,30000,35001,35002,\
         35003,35004,35005,35006,35007,35008,35009,40101,40301,40701,41001,41002,41003,41004,41005,49001,49002,49003,49004,\
         50000,51010,51020,51040,51044,51045,51080,51084,51085,51087,51089,51090,51095,51096,51097,51105,51110,51115,51120,\
         51125,51135,51145,51154,51155,51164,51165,51167,51175,51186,51206,51215,51224,51225,51235,51244,51245,51246,51255,\
         53001,55001,55002,55101,55102,55103,57001,57002,59301,59302,59303,59304,59305,59306,59307,59308,59309,70101,70201,70301]


def main(CC_q_filename, V1_CC_filename, output_filename):
    ''' ----------------------------------------------- List vars ------------------------------------------------ '''
    type_of_tuples = []
    num_types = 0
    n_men = 0
    n = 0

    n_age = 0
    n_ten = 0
    n_puma = 0

    aug_q = []
    aug_counts = []
    S_CC = []
    b_S_CC = []

    A = []

    io_stats = open(output_filename, "a")
    io_stats.write("-------------------------------------------------------------------------------------------------------------------------------------\n")
    io_stats.write("\nCreate V2 ---- start time %s\n" %(datetime.datetime.now()))
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS V2 CASCADE")
        cursor.execute("CREATE TABLE V2 AS SELECT h_id, PUMA10, TEN FROM " + h_table)

        cursor.execute("DROP TABLE IF EXISTS V1 CASCADE")
        cursor.execute("CREATE TABLE V1 AS SELECT * FROM " + p_table)
        cursor.execute("ALTER TABLE V1 ADD COLUMN PUMA10 INTEGER DEFAULT -1")
        cursor.execute("ALTER TABLE V1 ADD COLUMN TEN INTEGER DEFAULT -1")
        
        # Also, create the final relation, i.e., p_with_hid(p_id, AGEP, RELP, SEX, h_id)
        cursor.execute("DROP TABLE IF EXISTS p_with_hid CASCADE")
        cursor.execute("CREATE TABLE p_with_hid AS SELECT * FROM " + p_table)
        cursor.execute("ALTER TABLE p_with_hid ADD COLUMN h_id INTEGER DEFAULT -1")

        cursor.execute("SELECT COUNT(*) FROM " + h_table)
        n_hh = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM " + p_table)
        n_ppl = cursor.fetchone()[0]

        save_num_HHs_ppl(n_hh, n_ppl)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?\n")
        print(e)
    io_stats.write("Create V2 ---- end time %s\n" %(datetime.datetime.now()))
    io_stats.write("----------------------------------------------------------------------------\n")


    io_stats.write("CCs to Ax=b ---- start time %s\n" %(datetime.datetime.now()))
    save_time(datetime.datetime.now())

    V1_CCs = open(V1_CC_filename, "r")
    for line in V1_CCs:
        k_v = line.rstrip().split(":")

        l = k_v[0].split(",")
        constraint_l = []
        for i in range(5):
            if l[i]=='-1':
                constraint_l.append(-1)
            else:
                l[i] = l[i][1:-1].split("-")                                    # Remove "[", "]"
                l[i] = [int(j) for j in l[i]]
                constraint_l.append(l[i])
        S_CC.append(constraint_l)
        b_S_CC.append(int(k_v[1]))
    V1_CCs.close()

    set_num_S_CCs(len(b_S_CC))


    ''' --------------------------------------------------------------------------------------------------------------
                                    Get attr_intervalization for attributes in R1
    -------------------------------------------------------------------------------------------------------------- '''
    # 1. Store breaking points from the domain of attributes used in S_CC
    for constraint in S_CC:
        for i in range(num_R1_non_key_attrs):
            if constraint[i] != -1:
                for j in range(len(constraint[i])):
                    val = constraint[i][j]
                    if j == 1:
                        val += 1                                                # [0, 17] makes list [0, 18] for [0, 18), [18, ...)
                    if val not in attr_intervalization[i]:
                        attr_intervalization[i].append(val)
    # 2. Sort and adjust AGEP's intervals
    for i in range(num_R1_non_key_attrs):
        attr_intervalization[i].sort()
        if i == 0:                                                              # AGEP domain is [0, 114]
            if 0 not in attr_intervalization[i]:
                l = [0]
                l.extend(attr_intervalization[i])
                attr_intervalization[i] = l
            if 115 not in attr_intervalization[i]:
                attr_intervalization[i].append(115)
    n_age = len(attr_intervalization[0])

    ''' --------------------------------------------------------------------------------------------------------------
                            Get tuple types and merge using attrs that allow intervals in S_{CC}
    -------------------------------------------------------------------------------------------------------------- '''
    type_of_tuples = get_existing_tuple_types(p_table)
    print("\nType of tuples before binning by intervals = %s" %(len(type_of_tuples)))
    io_stats.write("\t\tType of tuples before binning by intervals = %s\n" %(len(type_of_tuples)))

    # Update the agep value to the corresponding interval from agep intervalization
    first_woman = False
    i = 0
    for tup in type_of_tuples:
        if tup[2] == 2 and not first_woman:
            i = 0
            first_woman = True
        u = attr_intervalization[0][i+1]
        while tup[0] >= u:
            i += 1
            u = attr_intervalization[0][i+1]
        l = attr_intervalization[0][i]
        u = attr_intervalization[0][i+1]
        tup[0] = [l, u-1]
    # Remove duplicates below
    no_dup = []
    for tup in type_of_tuples:
        if tup not in no_dup:
            no_dup.append(tup)
            if tup[2] == 1:
                n_men += 1
    type_of_tuples = no_dup
    num_types = len(type_of_tuples)
    print("Number of tuple types in R1 (using AGEP intervalization) = %s" %(num_types))
    io_stats.write("\t\tNumber of tuple types in R1 (using AGEP intervalization) = %s\n" %(num_types))
    n = num_types


    ''' --------------------------------------------------------------------------------------------------------------
                            Get active domain for attributes in R2, and total number of vars in IP
    -------------------------------------------------------------------------------------------------------------- '''
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        cursor.execute("SELECT array(SELECT DISTINCT TEN FROM " + h_table + " ORDER BY TEN)")
        attr_intervalization[3] = list(cursor.fetchone()[0])
        n_ten = len(attr_intervalization[3])
        if n_ten > 0:
            n = n * n_ten

        cursor.execute("SELECT array(SELECT DISTINCT PUMA10 FROM " + h_table + " ORDER BY PUMA10)")
        attr_intervalization[4] = list(cursor.fetchone()[0])
        n_puma = len(attr_intervalization[4])
        if n_puma > 0:
            n = n * n_puma

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?\n")
        print(e)

    print("\t\tDistinct values (TEN, PUMA10):\t%s\n\t\t%s\n" %(attr_intervalization[3], attr_intervalization[4]))
    print("Number of variables = %s" %(n))
    io_stats.write("\n\t\tNumber of variables = %2d\n" %(n))


    ''' --------------------------------------------------------------------------------------------------------------
                            Use S_CC and add the corresponding x_i's to populate rows in A and b
    ------------------------------------------------------------------------------------------------------------------
                            DO NOT augment input set of CCs with AGEP-RELP-SEX marginals from R_1
    -------------------------------------------------------------------------------------------------------------- '''
    aug_q = S_CC
    aug_counts = b_S_CC

    for i in aug_q:
        row = []

        if i[4] != -1:
            idx_puma = attr_intervalization[4].index(i[4][0])
            row = [0]*num_types*n_ten*idx_puma

        if i[0] == -1:
            row_sub = no_agep(i[1], i[2], type_of_tuples, n_men)
        else:
            row_sub = given_agep(i[0], i[1], i[2], type_of_tuples, n_men)

        if i[3] == -1:
            row.extend(row_sub*n_ten)
        elif i[3][0] == 1:
            row.extend(row_sub)
            if n_ten >= 2:
                row.extend([0]*int(n/(n_puma*n_ten) * (n_ten-1)))               # Ignore other TENs
        elif i[3][0] == 2:
            row.extend([0]*int(n/(n_puma*n_ten)))
            row.extend(row_sub)
            if n_ten >= 3:
                row.extend([0]*int(n/(n_puma*n_ten) * (n_ten-2)))               # Ignore other TENs
        elif i[3][0] == 3:
            row.extend([0]*int(2*n/(n_puma*n_ten)))
            row.extend(row_sub)
            if n_ten == 4:
                row.extend([0]*int(n/(n_puma*n_ten)))                           # Ignore other TENs ... n_ten - 3 is 1 here
        elif i[3][0] == 4:
            row.extend([0]*int(3*n/(n_puma*n_ten)))
            row.extend(row_sub)

        if i[4] != -1:
            num_pumas_rem = n_puma - attr_intervalization[4].index(i[4][0]) - 1
            row.extend([0]*int(n/n_puma)*num_pumas_rem)
        else:
            row.extend(row*(n_puma-1))

        A.append(row)

    print("\nSolving IP ....")
    prob = LpProblem("myProblem", LpMinimize)
    ppl_types = [i for i in range(n)]

    ppl_vars = LpVariable.dicts("person", ppl_types, lowBound=0, cat='Integer')
    #print(ppl_vars)

    prob += 0

    for i in range(len(A)):
        prob += lpSum([A[i][j]*ppl_vars[j] for j in ppl_types]) == aug_counts[i], "CC"+str(i)

    status = prob.solve()
    io_stats.write("\t\t******** STATUS = %s\n" %(LpStatus[prob.status]))
    print("\t\t******** STATUS = %s\n" %(LpStatus[prob.status]))

    x = [0 for i in range(n)]
    for v in prob.variables():
        #print(v.name, "=", v.varValue)
        if v.name != '__dummy':
            x[int(v.name.split("_")[1])] = int(v.varValue)

    io_stats.write("\tCCs to Ax=b ---- end time %s\n\n" %(datetime.datetime.now()))
    save_time("%s\n" %(datetime.datetime.now()))
    

    io_stats.write("\tAx=b to V1 ---- start time %s\n" %(datetime.datetime.now()))
    save_time(datetime.datetime.now())

    # Create UPDATE queries to run on V_1 in PostgreSQL on the basis of x. We know the values x[i]'s encode.
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        
        # AGEP, RELP, SEX, TEN, PUMA10
        for p in range(len(attr_intervalization[4])):
            for t in range(len(attr_intervalization[3])):
                for i in range(num_types):
                    x_i_val = x[p*n_ten*num_types + t*num_types + i]
                    if x_i_val > 0:
                        agep_val = type_of_tuples[i][0]
                        relp_val = str(type_of_tuples[i][1])
                        sex_val = str(type_of_tuples[i][2])
                        ten_val = str(attr_intervalization[3][t])
                        puma_val = str(attr_intervalization[4][p])

                        query = "WITH num_tuples_per_x AS (SELECT * FROM V1 WHERE PUMA10=-1 AND AGEP>=" + str(agep_val[0]) + \
                        " AND AGEP<=" + str(agep_val[1]) + " AND RELP=" + relp_val + " AND SEX=" + sex_val + " LIMIT " + \
                        str(x_i_val) + ") UPDATE V1 SET PUMA10=" + puma_val + ", TEN=" + ten_val + \
                        " FROM num_tuples_per_x WHERE V1.p_id=num_tuples_per_x.p_id"

                        cursor.execute(query)

                        print(query.split("AS ")[1].split(" FROM num_tuples_per_x")[0])
        save_time(datetime.datetime.now())
        # COMPLETE TUPLES WITHOUT ANY ASSIGNMENT IN V1
        cursor.execute("SELECT array(SELECT p_id FROM V1 WHERE PUMA10 = -1 AND TEN = -1)")
        tups_no_assign = list(cursor.fetchone()[0])
        print("\nNumber of tuples without assignment = %s\n" %(len(tups_no_assign)))
        for p_id in tups_no_assign:
            p = random.choice(dom_puma)
            t = random.choice(dom_ten)
            cursor.execute("UPDATE V1 SET PUMA10 = " + str(p) + ", TEN = " + str(t) + " WHERE p_id = " + str(p_id))

        io_stats.write("\tAx=b to V1 ---- end time %s\n\n" %(datetime.datetime.now()))
        save_time(datetime.datetime.now())

        # CHECK CC VIOLATIONS FOR CCs
        err = 0
        with open(CC_q_filename, "r") as q:
            for l in q:
                k_v = l.rstrip().split(":")
                pred = k_v[0]
                target_count = int(k_v[1])

                cursor.execute("SELECT COUNT(*) FROM V1 WHERE " + pred)
                ans = cursor.fetchone()[0]

                if target_count != ans:
                    if err == 0:
                        io_stats.write("\tCCs with target counts minus counts in V1\n")
                    io_stats.write("\t%5d - %5d = %5d \t%s\n" %(target_count, ans, target_count - ans, pred))
                    err += abs(target_count - ans)
        save_tot_L1_err("%s (CC)\n" %(err))
        
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?\n")
        print(e)
    
    
    io_stats.write("----------------------------------------------------------------------------\n\n\n\n\n")
    io_stats.close()


if __name__ == '__main__':
    main(CC_q_filename, V1_CC_filename, output_filename)
