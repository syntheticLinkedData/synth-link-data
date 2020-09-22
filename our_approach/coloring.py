import cc_sol_ver2
import datetime
import lp
import conn_str
import psycopg2
import networkx as nx
import random




def get_neighbors_of_vx(G, i):
    return list(G.neighbors(i))

def all_node_neighbors(G):
    print("Node neighbor lists are as follows:")
    for n, nbrs in G.adj.items():
        print("%s %s" %(n, nbrs))

def num_colors_used(coloring):
    n = -1
    for v, c in coloring.items():
        if (n < c):
            n = c
    print("#colors used = %2d" %(n+1))
    return n+1

def color_class_info(coloring):
    c_vx = {}
    for v, c in coloring.items():
        if (c not in c_vx):
            c_vx[c] = [v]
        else:
            c_vx[c].append(v)
    '''print("Color classes are as follows:")
    for c, l in c_vx.items():
        print("Color #%s: %s" %(c, l))'''
    return c_vx

def print_hist_sorted_by_size(hist):
    for size, num in sorted(hist.items(), key=lambda x: x[0]): 
        print("{} : {}".format(size, num))




def add_edges(G, l_ppl_1, l_ppl_2, S_DC):
    if l_ppl_1 == []:
        l_ppl_1 = G
    if l_ppl_2 == []:
        l_ppl_2 = G

    for i in l_ppl_1:
        if G.nodes[i]['relp'] == 0:
            for j in l_ppl_2:
                if not ((G.nodes[j]['relp'] >=0 and G.nodes[j]['relp'] <= 9) or G.nodes[j]['relp'] == 13 or G.nodes[j]['relp'] == 14):
                    continue

                #(Male HHer) No biological, adoptive or step- child can have an age outside of [A-69, A-12]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_1.SEX=1 \land t_2.RELP=2 \land t_2.AGE<t_1.AGE-69 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_1.SEX=1 \land t_2.RELP=2 \land t_2.AGE>t_1.AGE-12 \land t_1.h_id=t_2.h_id)
                #Similarly for t_2.RELP = 3, 4
                if 1 in S_DC and G.nodes[i]['sex'] == 1:
                    if ((G.nodes[j]['relp'] == 2 or G.nodes[j]['relp'] == 3 or G.nodes[j]['relp'] == 4) and \
                        (G.nodes[i]['agep'] - 69 > G.nodes[j]['agep'] or G.nodes[i]['agep'] - 12 < G.nodes[j]['agep'])):
                        G.add_edge(i, j)

                #(Female HHer) No biological, adoptive or step- child can have an age outside of [A-50, A-12]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_1.SEX=2 \land t_2.RELP=2 \land t_2.AGE<t_1.AGE-50 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_1.SEX=2 \land t_2.RELP=2 \land t_2.AGE>t_1.AGE-12 \land t_1.h_id=t_2.h_id)
                #Similarly for t_2.RELP = 3, 4
                elif 2 in S_DC and G.nodes[i]['sex'] == 2:
                    if ((G.nodes[j]['relp'] == 2 or G.nodes[j]['relp'] == 3 or G.nodes[j]['relp'] == 4) and \
                        (G.nodes[i]['agep'] - 50 > G.nodes[j]['agep'] or G.nodes[i]['agep'] - 12 < G.nodes[j]['agep'])):
                        G.add_edge(i, j)

                #No spouse or unmarried partner can have an age outside of [A-50, A+50]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=1 \land t_2.AGE<t_1.AGE-50 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=1 \land t_2.AGE>t_1.AGE+50 \land t_1.h_id=t_2.h_id)
                #Similarly for t_2.RELP = 13
                if 3 in S_DC and ((G.nodes[j]['relp'] == 1 or G.nodes[j]['relp'] == 13) and \
                    (G.nodes[i]['agep'] - 50 > G.nodes[j]['agep'] or G.nodes[i]['agep'] + 50 < G.nodes[j]['agep'])):
                    G.add_edge(i, j)

                #No sibling can have an age outside of [A-35, A+35]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=5 \land t_2.AGE<t_1.AGE-35 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=5 \land t_2.AGE>t_1.AGE+35 \land t_1.h_id=t_2.h_id)
                elif 4 in S_DC and ((G.nodes[j]['relp'] == 5) and \
                    (G.nodes[i]['agep'] - 35 > G.nodes[j]['agep'] or G.nodes[i]['agep'] + 35 < G.nodes[j]['agep'])):
                    G.add_edge(i, j)

                #No parent or parent-in-law can have an age outside of [A+12, A+115]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=6 \land t_2.AGE<t_1.AGE+12 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=6 \land t_2.AGE>t_1.AGE+115 \land t_1.h_id=t_2.h_id)
                #Similarly for t_2.RELP = 8
                elif 5 in S_DC and ((G.nodes[j]['relp'] == 6 or G.nodes[j]['relp'] == 8) and \
                    (G.nodes[i]['agep'] + 12 > G.nodes[j]['agep'] or G.nodes[i]['agep'] + 115 < G.nodes[j]['agep'])):
                    G.add_edge(i, j)

                #No grandchild can have an age outside of [A-115, A-30]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=7 \land t_2.AGE<t_1.AGE-115 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=7 \land t_2.AGE>t_1.AGE-30 \land t_1.h_id=t_2.h_id)
                elif 6 in S_DC and ((G.nodes[j]['relp'] == 7) and \
                    (G.nodes[i]['agep'] - 115 > G.nodes[j]['agep'] or G.nodes[i]['agep'] - 30 < G.nodes[j]['agep'])):
                    G.add_edge(i, j)

                #No son-/daughter-in-law can have an age outside of [A-69, A-1]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=9 \land t_2.AGE<t_1.AGE-69 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=9 \land t_2.AGE>t_1.AGE-1 \land t_1.h_id=t_2.h_id)
                elif 7 in S_DC and ((G.nodes[j]['relp'] == 9) and \
                    (G.nodes[i]['agep'] - 69 > G.nodes[j]['agep'] or G.nodes[i]['agep'] - 1 < G.nodes[j]['agep'])):
                    G.add_edge(i, j)

                #No foster-child can have an age outside of [A-69, A-12]
                #DC_1: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=14 \land t_2.AGE<t_1.AGE-69 \land t_1.h_id=t_2.h_id)
                #DC_2: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=14 \land t_2.AGE>t_1.AGE-12 \land t_1.h_id=t_2.h_id)
                elif 8 in S_DC and ((G.nodes[j]['relp'] == 14) and \
                    (G.nodes[i]['agep'] - 69 > G.nodes[j]['agep'] or G.nodes[i]['agep'] - 12 < G.nodes[j]['agep'])):
                    G.add_edge(i, j)

                #No two householders can share a household
                #DC: \forall t_1, t_2, \neg(t_1.RELP=0 \land t_2.RELP=0 \land t_1.h_id=t_2.h_id)
                elif 9 in S_DC and G.nodes[j]['relp'] == 0:
                    G.add_edge(i, j)

                #If A<30, then the number of grandchildren and son-/daughter-in-law in the household must be 0
                if 10 in S_DC and G.nodes[i]['agep'] < 30:
                    if (G.nodes[j]['relp'] == 7 or G.nodes[j]['relp'] == 9):
                        G.add_edge(i, j)

                #If A>94, then the number of parent and parent-in-law in the household must be 0
                elif 11 in S_DC and G.nodes[i]['agep'] > 94:
                    if (G.nodes[j]['relp'] == 6 or G.nodes[j]['relp'] == 8):
                        G.add_edge(i, j)

        #No two spouses or unmarried partners can share a household
        #DC: \forall t_1, t_2, \neg(t_1.RELP=1 \land t_2.RELP=1 \land t_1.h_id=t_2.h_id)
        #Similarly when both RELP values are 13, and when 1 is 13 but the other is 1
        elif 12 in S_DC and (G.nodes[i]['relp'] == 1 or G.nodes[i]['relp'] == 13):
            for j in l_ppl_2:
                if G.nodes[j]['relp'] == 1 or G.nodes[j]['relp'] == 13:
                    G.add_edge(i, j)




def get_nodes_desc_deg(G, l_ppl_uncolored):
    l = {}
    for v in l_ppl_uncolored:
        l[v] = G.degree(v)
    l = sorted(l.items(), key=lambda x: x[1], reverse=True)
    l = [l[i][0] for i in range(len(l))]
    #print("Node Degree")
    #print(l)
    return l

def largest_first_coloring(G, l_ppl_uncolored, h_l, h_u, coloring):
    l_nodes_desc_degree = get_nodes_desc_deg(G, l_ppl_uncolored)

    can_use = [j for j in range(h_l, h_u)]
    #print("h_id interval: %s\n" %(can_use))

    skipped_vxs = []
    for vx in l_nodes_desc_degree:
        nbrs = get_neighbors_of_vx(G, vx)
        #print("Neighbors: %s\n" %(nbrs))
        
        used = []
        for nbr in nbrs:
            if nbr in coloring:                         # cannot use such colors
                used.append(coloring[nbr])
        
        diff = list(set(can_use) - set(used))           # find a color that has not been used on a nbr
        #print(str(diff)+"\n")

        if len(diff) != 0:
            coloring[vx] = min(diff)
        else:                                           # skip vx in this run of the coloring algorithm
            skipped_vxs.append(vx)
            continue
    return coloring, skipped_vxs




# M2 helper to compute L1 CC error
def get_L1_CC_err(rank):
    err = 0
    i = 0

    q = open(CC_q_filename, "r")
    for line in q:
        k_v = line.rstrip().split(":")
        cursor.execute("SELECT COUNT(*) FROM V1 WHERE " + k_v[0])
        err += abs(int(k_v[1]) - cursor.fetchone()[0])*rank[i]
        i += 1
    q.close()

    return err

# Greedy metric M2 - Find p and t that minimize the L1 error for CC violations
def create_color_skip_inval_vx_M2(vx_i, h_id, rank):
    puma10 = lp.get_intervals(4)
    ten = lp.get_intervals(3)
    p = random.choice(puma10)
    t = random.choice(ten)
    
    try:
        min_viol = get_L1_CC_err(rank)
        for p_i in puma10:
            for t_i in ten:
                cursor.execute("INSERT INTO V2(h_id, PUMA10, TEN) VALUES(" + str(h_id) + "," + str(p_i) + "," + str(t_i) + ")")
                err_post = get_L1_CC_err(rank)

                cursor.execute("DELETE FROM V2 WHERE h_id=" + str(h_id))

                #io_stats.write(str(p_i)+" "+str(t_i)+" "+str(min_viol)+" "+str(err_post)+"\n")
                if err_post < min_viol:
                    min_viol = err_post
                    p = p_i
                    t = t_i
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
    io_stats.write(str(p)+" "+str(t)+"\n")
    return p, t








CC_q_filename = "CC_queries_all_10_good.txt"
V1_CC_filename = "V1_CC_dict_all_10_good.txt"
output_filename = "details.txt"

l_ppl_in_diff_puma_ten, l_hhs_in_diff_puma_ten = cc_sol_ver2.main(CC_q_filename, V1_CC_filename, output_filename)

S_DC = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]                  # All DCs


'''------------------------------------------------------------------------------------------------------------------------
                                                h_id UNKNOWN for our problem
---------------------------------------------------------------------------------------------------------------------------
                                CONSTRUCT CONFLICT GRAPHS AND COLOR THEM TO GET h_id ASSIGNMENT
------------------------------------------------------------------------------------------------------------------------'''
io_stats = open(output_filename, "a")
lp.save_time(datetime.datetime.now())

all_coloring = {}
all_hid = []

new_hh_ids = -1

h_l = 0
h_u = 0

for (puma, ten), l_ppl in l_ppl_in_diff_puma_ten.items():
    if puma == -1:
        continue
    if (puma, ten) in l_hhs_in_diff_puma_ten:
        h_u += len(l_hhs_in_diff_puma_ten[(puma, ten)])
        all_hid.extend(l_hhs_in_diff_puma_ten[(puma, ten)])                         # Referring to h_ids by interval [h_l, h_u-1] .. diff "name"
    
    io_stats.write("\n\n\n\n---------------------------PUMA10 = %3d - TEN = %1d---------------------------\n" %(puma, ten))
    io_stats.write("h_id range = %s-%s\n" %(h_l, h_u-1))
    io_stats.write("Number of people = %s\n" %(len(l_ppl)))
    
    io_stats.write("\tConstruct G_c and color ---- start time %s\n" %(datetime.datetime.now()))

    G = nx.Graph()                                                                  # undirected simple graph
    # Add nodes ---------------------------------------------------------------------------------------------------------------
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        for i in l_ppl:
            cursor.execute("SELECT * FROM V1 WHERE p_id=" + str(i))
            row = cursor.fetchone()
            G.add_node(i, p_id=row[0], agep=row[1], relp=row[2], sex=row[3])

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
    # Add edges ---------------------------------------------------------------------------------------------------------------
    add_edges(G, l_ppl, l_ppl, S_DC)


    '''------------------------------------------------------------------------------------------------------------------------
                                                            Color graph G
    ---------------------------------------------------------------------------------------------------------------------------
     -> For vx i, the colors available (initially) are [h_l, h_u)
    ------------------------------------------------------------------------------------------------------------------------'''
    all_coloring, skipped_vxs = largest_first_coloring(G, l_ppl, h_l, h_u, all_coloring)
    if len(skipped_vxs) > 0:
        #print(puma, "\t", ten)
        h_neg_l = new_hh_ids-len(skipped_vxs)
        #print(h_neg_l, new_hh_ids)
        all_coloring, temp = largest_first_coloring(G, skipped_vxs, h_neg_l, new_hh_ids, all_coloring)
        new_hh_ids = h_neg_l
    
    
    h_l = h_u
    io_stats.write("\tConstruct G_c and color ---- end time %s\n\n" %(datetime.datetime.now()))
    
    
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
            if i not in skipped_vxs:
                query = "UPDATE p_with_hid SET h_id=" + str(all_hid[all_coloring[i]]) + " WHERE p_id=" + str(i)
                cursor.execute(query)
            else:
                query = "UPDATE p_with_hid SET h_id=" + str(all_coloring[i]) + " WHERE p_id=" + str(i)
                cursor.execute(query)
                # Create new HHs for vertices that were skipped in the coloring IF IT DOES NOT ALREADY EXIST
                cursor.execute("SELECT COUNT(*) FROM V2 WHERE h_id="+str(all_coloring[i]))
                if cursor.fetchone()[0] == 0:
                    query = "INSERT INTO V2(h_id, PUMA10, TEN) VALUES(" + str(all_coloring[i]) + "," + str(puma) + "," + str(ten) + ")"
                    cursor.execute(query)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
    io_stats.write("\tUpdate p_with_hid ---- end time %s" %(datetime.datetime.now()))
    io_stats.write("\n----------------------------------------------------------------------------\n")


#hhs = color_class_info(all_coloring)                                           # Find out who is in each HH so far
'''------------------------------------------------------------------------------------------------------------------------
                    CONSTRUCT CONFLICT GRAPHS AND COLOR THEM TO GET h_id ASSIGNMENT
------------------------------------------------------------------------------------------------------------------------'''
if (-1, -1) in l_ppl_in_diff_puma_ten.keys():
    new_hh_ids -= 1                                                             # Any new homes must have id smaller than id of any previously created new homes
    full_G = nx.Graph()                                                         # Graph over all tuples in R1 (or V1)

    rank = [1 for i in range(lp.get_num_S_CCs())]                               # !!!!!

    io_stats.write("\n\n\n\n---------------------------PUMA10 = -1 - TEN = -1---------------------------\n")
    l_ppl_invalid = l_ppl_in_diff_puma_ten[(-1, -1)]
    io_stats.write("Number of people = %2d\n" %(len(l_ppl_invalid)))
    
    io_stats.write("\tConstruct G_c and color ---- start time %s\n" %(datetime.datetime.now()))

    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        p_ids = [j for i in l_ppl_in_diff_puma_ten.values() for j in i]
        for i in p_ids:                                                         # Create vertex for ALL tuples in R1 (or V1)
            cursor.execute("SELECT * FROM V1 WHERE p_id=" + str(i))
            row = cursor.fetchone()
            full_G.add_node(i, p_id=row[0], agep=row[1], relp=row[2], sex=row[3])

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)

    # We only want edges incident to invalid vxs
    add_edges(full_G, l_ppl_invalid, [], S_DC)
    add_edges(full_G, [], l_ppl_invalid, S_DC)
    
    
    '''------------------------------------------------------------------------------------------------------------------------
                                    Color graph G
    ---------------------------------------------------------------------------------------------------------------------------
     -> For vx i, the colors available (initially) are [h_l, h_u)
    ------------------------------------------------------------------------------------------------------------------------'''
    all_coloring, skipped_vxs = largest_first_coloring(full_G, l_ppl_invalid, 0, len(all_hid), all_coloring)
    for vx in skipped_vxs:
        all_coloring[vx] = new_hh_ids
        new_hh_ids -= 1
    io_stats.write("\tConstruct G_c and color ---- end time %s\n\n" %(datetime.datetime.now()))
    

    io_stats.write("\tUpdate p_with_hid for people with PUMA10=-1 ---- start time %s\n" %(datetime.datetime.now()))
    '''------------------------------------------------------------------------------------------------------------------------
                                        Update h_id value in p_with_hid for people with PUMA10=-1
    ------------------------------------------------------------------------------------------------------------------------'''
    try:
        connect_str = conn_str.get_conn_str()
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        for i in l_ppl_invalid:
            if i not in skipped_vxs:
                cursor.execute("UPDATE p_with_hid SET h_id=" + str(all_hid[all_coloring[i]]) + " WHERE p_id=" + str(i))
            else:
                cursor.execute("UPDATE p_with_hid SET h_id=" + str(all_coloring[i]) + " WHERE p_id=" + str(i))
                # Create new HHs for vertices that were skipped in the coloring
                puma10, ten = create_color_skip_inval_vx_M2(full_G.nodes[i], all_coloring[i], rank)
                
                cursor.execute("INSERT INTO V2(h_id, PUMA10, TEN) VALUES(" + str(all_coloring[i]) + "," + str(puma10) + "," + str(ten) + ")")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
    io_stats.write("\tUpdate p_with_hid for people with PUMA10=-1 ---- end time %s\n" %(datetime.datetime.now()))
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
    for line in q:
        line = line.rstrip()
        k_v = line.split(":")
        cursor.execute(k_v[0])
        io_stats.write(str(cursor.fetchone()[0]) + "\t" + k_v[1] + "\n")
    q.close()
    io_stats.write("\n")

    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)




'''---------------------------------------------------------------------------------------------------------------------'''
lp.save_tot_L1_err("%s\n----------------------------------------\n" %(err))
io_stats.write("\n\n\n\n\n")
io_stats.close()
