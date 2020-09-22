import sys
#import matplotlib.pyplot as plt
import networkx as nx
import conn_str
import psycopg2
import datetime
import lp
import copy
import random




def plot_forest(T):
	labels = nx.get_node_attributes(T, 'name')
	nx.draw_circular(T, labels=labels, node_size=500, font_size=8)
	#labels = nx.draw_networkx_labels(T, pos=nx.circular_layout(G), font_size=8)
	plt.show()




def update_V1_all_disjoint(S_CC, T):
	try:
		connect_str = conn_str.get_conn_str()
		conn = psycopg2.connect(connect_str)
		cursor = conn.cursor()

		for vx in T:
			cc = S_CC[vx]
			b = T.nodes[vx]['target']

			query = "WITH num_tuples_per_x AS (SELECT * FROM V1 WHERE PUMA10 = -1 AND TEN = -1"
			for i in range(num_R1_non_key_attrs):
				if cc[i] != -1:
					attr = attr_names[i]
					if i == 0:
						query += " AND " + attr + " >= " + str(cc[i][0]) + " AND " + attr + " <= " + str(cc[i][1])
					else:
						query += " AND " + attr + " = " + str(cc[i][0])
			query += " LIMIT " + str(b) + ") UPDATE V1 SET "
			for j in range(num_R2_non_key_attrs):
				if cc[i+1+j] != -1:
					query += attr_names[i+1+j] + " = " + str(cc[i+1+j][0]) + ", "
			query = query[:-2] + " FROM num_tuples_per_x WHERE V1.p_id=num_tuples_per_x.p_id"

			print("\t\t", query)
			cursor.execute(query)

		conn.commit()
		cursor.close()
		conn.close()
	except Exception as e:
		print("Uh oh, can't connect. Invalid dbname, user or password?\n")
		print(e)
	return

def update_V1_root(S_CC, T, r):
	try:
		connect_str = conn_str.get_conn_str()
		conn = psycopg2.connect(connect_str)
		cursor = conn.cursor()

		cc = S_CC[r]
		b = T.nodes[r]['target']
		b_sum_children = 0

		query = "WITH num_tuples_per_x AS (SELECT * FROM V1 WHERE PUMA10 = -1 AND TEN = -1"
		for i in range(num_R1_non_key_attrs):
			if cc[i] != -1:
				attr = attr_names[i]
				if i == 0:
					query += " AND " + attr + " >= " + str(cc[i][0]) + " AND " + attr + " <= " + str(cc[i][1])
				else:
					query += " AND " + attr + " = " + str(cc[i][0])
		# When not at the leaf node, must update the SELECT condition
		for child in T.successors(r):							# Path graph == 1 successor; OTHERWISE CC at root in subtree
			cc_child = S_CC[child]

			query += " AND NOT("
			for i in range(num_R1_non_key_attrs):
				if cc_child[i] != -1:
					attr = attr_names[i]
					if i == 0:
						query += attr + " >= " + str(cc_child[i][0]) + " AND " + attr + " <= " + str(cc_child[i][1]) + " AND "
					else:
						query += attr + " = " + str(cc_child[i][0]) + " AND "
			query = query[:-5] + ")"
			
			b_sum_children += T.nodes[child]['target']
		# Must use difference between targets of the current CC and it's successor CC in T
		query += " LIMIT " + str(b - b_sum_children) + ") UPDATE V1 SET "
		for j in range(num_R2_non_key_attrs):
			if cc[num_R1_non_key_attrs + j] != -1:
				query += attr_names[num_R1_non_key_attrs + j] + " = " + str(cc[num_R1_non_key_attrs + j][0]) + ", "
		query = query[:-2] + " FROM num_tuples_per_x WHERE V1.p_id=num_tuples_per_x.p_id"

		print("\t\t", query)
		cursor.execute(query)

		conn.commit()
		cursor.close()
		conn.close()
	except Exception as e:
		print("Uh oh, can't connect. Invalid dbname, user or password?\n")
		print(e)
	return




def view_completion(S_CC, T):
	# First check: All pairs disjoint == T has no edges
	if len(T.edges()) == 0:
		print("--------------------\nTree with vertices and edges: ", T.nodes(), ", ", T.edges())
		print("\t\tAll disjoint")
		update_V1_all_disjoint(S_CC, T)
		return

	# Alas, must RECURSE ...
	roots = []
	for vx in T:
		if T.in_degree(vx) == 0:
			roots.append(vx)
	# RECURSE ...
	for r in roots:
		tree = nx.dfs_tree(T, r)
		print("--------------------\nTree with vertices and edges: ", tree.nodes(), ", ", tree.edges())
		tree.nodes[r]['target'] = T.nodes[r]['target']
		for child in T.successors(r):
			tree.nodes[child]['target'] = T.nodes[child]['target']

			subtree = nx.dfs_tree(T, child)
			for vx in subtree:
				subtree.nodes[vx]['target'] = T.nodes[vx]['target']
			view_completion(S_CC, subtree)
		update_V1_root(S_CC, T, r)
	return




attr_names = ["AGEP", "RELP", "SEX", "TEN", "PUMA10"]
num_R1_non_key_attrs = 3
num_R2_non_key_attrs = 2

dom_ten = [1, 2, 3, 4]
size_dom_ten = len(dom_ten)

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
size_dom_puma = len(dom_puma)

p_table = "p_allpumas_10"
h_table = "h_allpumas_10"


def main(CC_q_filename, V1_CC_filename, output_filename):
	''' ----------------------------------------------- List vars ------------------------------------------------ '''
	S_CC = []
	b_S_CC = []
	total = 0
	num_CCs = 0

	disjoint_CC = {}
	intersect_with = {}
	T = nx.DiGraph()

	intersecting_S_CC = []
	intersecting_b_S_CC = []

	edges_to_remove = []

	used_tens = []													# TEN vals in S_CC
	used_tens_without_puma = []
	tens_onlyWithPUMA10_or_unused = []

	used_pumas = []													# PUMA10 vals in S_CC
	used_pumas_without_ten = []
	pumas_onlyWithTEN_or_unused = []

	ten_puma_combos_in_CCs = []										# TEN-PUMA10 combinations in S_CC
	ten_dict = {}													# Combinations "indexed" by TEN vals
	puma_dict = {}													# Combinations "indexed" by PUMA10 vals

	unused_combos_in_CCs_by_puma = []								# Candidate PUMA10 vals for tuples missing assignment after lp.py

	err = -1

	l_ppl_in_diff_puma_ten = {}
	l_hhs_in_diff_puma_ten = {}


	io_stats = open(output_filename, "a+")
	io_stats.write("-------------------------------------------------------------------------------------------------------------------------------------\n")
	io_stats.write("Create V2 ---- start time %s\n" %(datetime.datetime.now()))
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
		lp.save_num_HHs_ppl(n_hh, n_ppl)

		'''cursor.execute("SELECT array(SELECT DISTINCT TEN FROM " + h_table + " ORDER BY TEN)")
		active_tens = list(cursor.fetchone()[0])
		
		cursor.execute("SELECT array(SELECT DISTINCT PUMA10 FROM " + h_table + " ORDER BY PUMA10)")
		active_pumas = list(cursor.fetchone()[0])'''
		
		conn.commit()
		cursor.close()
		conn.close()
	except Exception as e:
		print("Uh oh, can't connect. Invalid dbname, user or password?\n")
		print(e)
	io_stats.write("Create V2 ---- end time %s\n" %(datetime.datetime.now()))
	io_stats.write("----------------------------------------------------------------------------\n\n")


	''' --------------------------------------------------------------------------------------------------------------
	                                					Read S_CC
	-------------------------------------------------------------------------------------------------------------- '''
	V1_CCs = open(V1_CC_filename, "r")

	for line in V1_CCs:
		k_v = line.rstrip().split(":")
		total += int(k_v[1])
		l = k_v[0].split(",")
		
		constraint_l = []
		t_p_combo = []
		for i in range(5):
			if l[i] == '-1':
				constraint_l.append(-1)
				if i == 3 or i == 4:
					t_p_combo.append(-1)
			else:
				l[i] = l[i][1:-1].split("-")					# Remove "[", "]"
				l[i] = [int(j) for j in l[i]]
				constraint_l.append(l[i])
				# Populate used_tens and used_pumas
				if i == 3:
					if l[i][0] not in used_tens:
						used_tens.append(l[i][0])
					t_p_combo.append(l[i][0])
				elif i == 4:
					if l[i][0] not in used_pumas:
						used_pumas.append(l[i][0])
					t_p_combo.append(l[i][0])
		if -1 not in t_p_combo and t_p_combo not in ten_puma_combos_in_CCs:
			ten_puma_combos_in_CCs.append(t_p_combo)
		else:
			ten = t_p_combo[0]
			puma = t_p_combo[1]
			if ten != -1 and ten not in used_tens_without_puma:
				used_tens_without_puma.append(ten)
			if puma != -1 and puma not in used_pumas_without_ten:
				used_pumas_without_ten.append(puma)

		S_CC.append(constraint_l)
		b_S_CC.append(int(k_v[1]))
	V1_CCs.close()
	num_CCs = len(S_CC)

	for i in range(num_CCs):
		print("%5d. %s" %(i, S_CC[i]))
	print("\nTotal CC target = %s\n" %(total))

	unused_tens = sorted(list(set(dom_ten).difference(set(used_tens))))
	unused_pumas = sorted(list(set(dom_puma).difference(set(used_pumas))))
	print("\nTEN values NOT used in S_CC: %s\n" %(unused_tens))
	print("PUMA10 values NOT used in S_CC: %s\n" %(unused_pumas))
	print("TEN values used WITHOUT PUMA10 in some CC: %s\n" %(used_tens_without_puma))
	print("PUMA10 values used WITHOUT TEN in some CC: %s\n\n" %(used_pumas_without_ten))

	for [t, p] in ten_puma_combos_in_CCs:
		#if t not in used_tens_without_puma:
		if t not in ten_dict:
			ten_dict[t] = [p]
		else:
			ten_dict[t].append(p)

		#if p not in used_pumas_without_ten:
		if p not in puma_dict:
			puma_dict[p] = [t]
		else:
			puma_dict[p].append(t)
	
	pumas_onlyWithTEN_or_unused = list(set(dom_puma).difference(set(used_pumas_without_ten)))
	tens_onlyWithPUMA10_or_unused = list(set(dom_ten).difference(set(used_tens_without_puma)))
	for p in pumas_onlyWithTEN_or_unused:
		# If p does not appear with every TEN val OR p is not used and there exist TEN vals that appear ONLY WITH PUMA10 in S_CC
		if (p in puma_dict and len(puma_dict[p]) < size_dom_ten) or (p in unused_pumas and len(tens_onlyWithPUMA10_or_unused) > 0):
			unused_combos_in_CCs_by_puma.append(p)


	''' --------------------------------------------------------------------------------------------------------------
	                                				Construct T and \cap_i
	-------------------------------------------------------------------------------------------------------------- '''
	io_stats.write("Pairwise comparisons on S_CC and constructing T ---- start time %s\n" %(datetime.datetime.now()))
	lp.save_time(datetime.datetime.now())

	for i in range(num_CCs):
		T.add_node(i, name="CC"+str(i), target=b_S_CC[i])

	disjoint_CC = {i:[] for i in range(num_CCs)}
	for i in range(num_CCs):
		for j in range(num_CCs):
			if i != j:
				R1_i = S_CC[i][:num_R2_non_key_attrs+1]
				R1_j = S_CC[j][:num_R2_non_key_attrs+1]

				R2_i = S_CC[i][-1*num_R2_non_key_attrs:]
				R2_j = S_CC[j][-1*num_R2_non_key_attrs:]

				disjoint = False
				contain = True
				# R1
				for k in range(num_R1_non_key_attrs):
					R1_i_attr = R1_i[k]
					R1_j_attr = R1_j[k]

					if R1_i_attr != -1 and R1_j_attr != -1:
						# Disjoint on R1? Enough to disagree on ONE attr used in the CCs
						if (k == 0 and (R1_i_attr[1] < R1_j_attr[0] or R1_i_attr[0] > R1_j_attr[1])) or \
							(k != 0 and R1_i_attr != R1_j_attr):
							disjoint = True
						# Contained on R1? Then check if contained on R2!
						if not((k == 0 and R1_i_attr[0] >= R1_j_attr[0] and R1_i_attr[1] <= R1_j_attr[1]) or \
							(k != 0 and R1_i_attr == R1_j_attr)):
							contain = False
					elif R1_j_attr != -1 and R1_i_attr == -1:
						contain = False
				# R2
				if contain:
					for k in range(num_R2_non_key_attrs):
						R2_i_attr = R2_i[k]
						R2_j_attr = R2_j[k]

						# If R1 preds match but disjoint R2 preds ----> Pair of CCs is disjoint, and NOT intersecting
						if R1_i == R1_j:
							if R2_i_attr != -1 and R2_j_attr != -1 and R2_i_attr != R2_j_attr:
								disjoint = True
								contain = False
								break
						# If R1_i is more restrictive than R1_j, BUT R2_i is not more restrictive than R2_j ----> NOT contained
						else:
							if (R2_i_attr != -1 and R2_j_attr != -1 and R2_i_attr != R2_j_attr) or \
								(R2_j_attr != -1 and R2_i_attr == -1):
								contain = False
								break
				# (Disjoint) Add pairs to dict
				if disjoint:
					disjoint_CC[i].append(j)
				# (Containment) Add edge from CC_j to CC_i IF CC_i\subseteq CC_j AND they do NOT contradict on R2
				elif not disjoint and contain:
					print("Add edge %s -> %s" %(j, i))
					T.add_edge(j, i)
	print()

	for i in range(num_CCs):
		for j in range(num_CCs):
			if i != j:
				if j not in disjoint_CC[i] and not(T.has_edge(i, j) or T.has_edge(j, i)):
					if i not in intersect_with:
						intersect_with[i] = [j]
					else:
						intersect_with[i].append(j)


	''' ----------------------------------- Find and remove redundant edges from T ----------------------------------- '''
	#plot_forest(T)
	list_of_edges = copy.deepcopy(T.edges())
	for (u, v) in list_of_edges:
		T.remove_edge(u, v)
		#plot_forest(T)
		if not nx.has_path(T, u, v):							# If removing the edge disconnects v from u, add edge back
			T.add_edge(u, v)
	#plot_forest(T)
	io_stats.write("Pairwise comparisons on S_CC and constructing T ---- end time %s\n\n" %(datetime.datetime.now()))
	lp.save_time(datetime.datetime.now())

	''' --------------------------------------------------------------------------------------------------------------
	                                			ViewCompletion (hierarchical)
	-------------------------------------------------------------------------------------------------------------- '''
	# Find "good" trees T_i in the forest T to recurse on
	vxs = copy.deepcopy(T.nodes())
	for vx in vxs:
		if vx in T and T.in_degree(vx) == 0:					# At ROOT!
			bad_T_i = False

			comp_nodes = list(nx.dfs_postorder_nodes(T, vx))	# Get nodes in root's tree
			for vx in comp_nodes:
				if vx in intersect_with:
					bad_T_i = True
					break
			if bad_T_i:											# T_i bad ----> Remove T_i from T
				for vx in comp_nodes:
					T.remove_node(vx)
					''' --------------------------- S_CC and b_S_CC for intersecting CCs ------------------------- '''
					intersecting_S_CC.append(S_CC[vx])
					intersecting_b_S_CC.append(b_S_CC[vx])
					print("%5d %s" %(intersecting_b_S_CC[-1], intersecting_S_CC[-1]))

	#plot_forest(T)

	# START V1 COMPLETION ...
	io_stats.write("Algo 1.1 ---- start time %s\n" %(datetime.datetime.now()))
	lp.save_time(datetime.datetime.now())
	view_completion(S_CC, T)

	try:
		connect_str = conn_str.get_conn_str()
		conn = psycopg2.connect(connect_str)
		cursor = conn.cursor()

		#--------------------------------- FIX tuples with TEN but no PUMA10 value -----------------------------------
		cursor.execute("SELECT p_id, TEN FROM V1 WHERE PUMA10 = -1 AND TEN != -1")
		y_ten_n_puma = list(cursor.fetchall())
		for tup in y_ten_n_puma:
			id = tup[0]
			ten = tup[1]

			puma = -1
			if ten in ten_dict:
				puma_candidates = list(set(dom_puma).difference(set(ten_dict[ten])))
				if len(puma_candidates) > 0:
					puma = random.choice(puma_candidates)
				else:
					print("\n\nNo TEN options\n\n")
			else:
				puma = random.choice(pumas_onlyWithTEN_or_unused)

			query = "UPDATE V1 SET PUMA10 = " + str(puma) + " WHERE p_id = " + str(id)
			cursor.execute(query)

		#---------------------------------- FIX tuples with PUMA10 but no TEN value ----------------------------------
		cursor.execute("SELECT p_id, PUMA10 FROM V1 WHERE PUMA10 != -1 AND TEN = -1")
		n_ten_y_puma = list(cursor.fetchall())
		for tup in n_ten_y_puma:
			id = tup[0]
			puma = tup[1]

			ten = -1
			if puma in puma_dict:
				ten_candidates = list(set(dom_ten).difference(set(puma_dict[puma])))
				if len(ten_candidates) > 0:
					ten = random.choice(ten_candidates)
				else:
					print("\n\nNo PUMA10 options\n\n")
			else:
				ten = random.choice(tens_onlyWithPUMA10_or_unused)

			query = "UPDATE V1 SET TEN = " + str(ten) + " WHERE p_id = " + str(id)
			cursor.execute(query)

		io_stats.write("Algo 1.1 ---- end time %s\n\n" %(datetime.datetime.now()))
		lp.save_time(datetime.datetime.now())


		# CHECK CC VIOLATIONS
		err = 0
		with open(CC_q_filename, "r") as q:
			lines = q.readlines()
			for i in range(len(lines)):
				l = lines[i]
				k_v = l.rstrip().split(":")
				pred = k_v[0]
				target_count = int(k_v[1])

				cursor.execute("SELECT COUNT(*) FROM V1 WHERE " + pred)
				ans = cursor.fetchone()[0]

				if i in T and target_count != ans:
					if err == 0:
						io_stats.write("\t(Unsatisfied disjoint or contained) CCs with target counts minus counts in V1\n")
					io_stats.write("\t%5d - %5d = %5d \t%s\n" %(target_count, ans, target_count - ans, pred))
					err += abs(target_count - ans)
		lp.save_tot_L1_err("%s\n" %(err))

		conn.commit()
		cursor.close()
		conn.close()
	except Exception as e:
		print("Uh oh, can't connect. Invalid dbname, user or password?")
		print(e)

	io_stats.write("----------------------------------------------------------------------------\n\n")
	io_stats.close()
	

	''' --------------------------------------------------------------------------------------------------------------
	                                		Pass any intersecting CCs to lp.py
	-------------------------------------------------------------------------------------------------------------- '''
	if len(intersecting_S_CC) > 0:
		lp.main(num_R1_non_key_attrs, intersecting_S_CC, intersecting_b_S_CC, p_table, h_table, CC_q_filename, output_filename)


	''' --------------------------------------------------------------------------------------------------------------
	1.					Assign unused TEN-PUMA10 combo to tuples in V1 to complete assignment
	------------------------------------------------------------------------------------------------------------------
	2.												Check CC violations
	-------------------------------------------------------------------------------------------------------------- '''
	io_stats = open(output_filename, "a+")
	try:
		connect_str = conn_str.get_conn_str()
		conn = psycopg2.connect(connect_str)
		cursor = conn.cursor()


		io_stats.write("\n\nPartition people and households by PUMA10-TEN ---- start time %s\n" %(datetime.datetime.now()))
		lp.save_time("%s" %(datetime.datetime.now()))
		# CREATE A DICTIONARY THAT PARTITIONS PEOPLE INTO (PUMA, TEN)
		cursor.execute("SELECT p_id, TEN, PUMA10 FROM V1 WHERE PUMA10 != -1 AND TEN != -1")
		rows = cursor.fetchall()
		for row in rows:
			p_id = row[0]
			ten = int(row[1])
			puma = row[2]
			if (puma, ten) not in l_ppl_in_diff_puma_ten:
				l_ppl_in_diff_puma_ten[(puma, ten)] = [p_id]
			else:
				l_ppl_in_diff_puma_ten[(puma, ten)].append(p_id)
		# CREATE A DICTIONARY THAT PARTITIONS HHs INTO (PUMA, TEN)
		cursor.execute("SELECT h_id, PUMA10, TEN FROM V2")
		rows = cursor.fetchall()
		for row in rows:
			h_id = row[0]
			puma = row[1]
			ten = int(row[2])
			if (puma, ten) not in l_hhs_in_diff_puma_ten:
				l_hhs_in_diff_puma_ten[(puma, ten)] = [h_id]
			else:
				l_hhs_in_diff_puma_ten[(puma, ten)].append(h_id)


		#----------------------------------- Complete partially filled tuples in V1 ----------------------------------
		cursor.execute("SELECT p_id FROM V1 WHERE PUMA10 = -1 AND TEN = -1")
		miss_assign_V1 = list(cursor.fetchall())
		if len(miss_assign_V1) > 0 and len(unused_combos_in_CCs_by_puma) > 0:
			print("\n\nNumber of tuples without assignment = %s\n\n" %(len(miss_assign_V1)))
			io_stats.write("\tComplete V1 assignment ---- start time %s\n" %(datetime.datetime.now()))
			#------------------------------------ Create list of TEN-PUMA10 combos -----------------------------------
			combos = []
			for p in unused_combos_in_CCs_by_puma:
				if p in puma_dict:
					ts = list(set(dom_ten).difference(set(puma_dict[p])))
				else:
					ts = tens_onlyWithPUMA10_or_unused
				for t in ts:
					combos.append([t, p])
			print("Combinations valid for tuples missing assignment after lp.py: %s\n\n" %(combos))
			io_stats.write("\t\tConstructing combos (end) %s\n" %(datetime.datetime.now()))

			for tup in miss_assign_V1:
				p_id = tup[0]
				[ten, puma] = random.choice(combos)
				if (puma, ten) not in l_ppl_in_diff_puma_ten:
					l_ppl_in_diff_puma_ten[(puma, ten)] = [p_id]
				else:
					l_ppl_in_diff_puma_ten[(puma, ten)].append(p_id)

			io_stats.write("\tComplete V1 assignment ---- end time %s\n" %(datetime.datetime.now()))
		else:
			print("\n\nAll TEN-PUMA10 combinations used in S_CC\n\n")
		
		io_stats.write("Partition people and households by PUMA10-TEN ---- end time %s\n" %(datetime.datetime.now()))
		lp.save_time("%s\n" %(datetime.datetime.now()))
		
		# CHECK CC VIOLATIONS
		io_stats.write("\n\nCCs with target counts minus counts in solution\n")
		with open(CC_q_filename, "r") as q:
			err = 0
			for l in q:
				k_v = l.rstrip().split(":")
				pred = k_v[0]
				target_count = int(k_v[1])

				cursor.execute("SELECT COUNT(*) FROM V1 WHERE " + pred)
				ans = cursor.fetchone()[0]

				io_stats.write("%5d - %5d = %5d \t%s\n" %(target_count, ans, target_count - ans, pred))
				err += abs(target_count - ans)
		lp.save_tot_L1_err("%s (Step 1 final)\n" %(err))

		conn.commit()
		cursor.close()
		conn.close()
	except Exception as e:
		print("Uh oh, can't connect. Invalid dbname, user or password?")
		print(e)


	io_stats.close()
	return l_ppl_in_diff_puma_ten, l_hhs_in_diff_puma_ten




if __name__ == '__main__':
	main(CC_q_filename, V1_CC_filename, output_filename)
