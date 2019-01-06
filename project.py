import operator
import numpy as np


# to do solve communication range problem
const=0.0009
class Node():
    def __init__(self,distance,tables,com_range,isclusterhead=0):
        self.isclusterhead = isclusterhead
        self.distance=distance
        self.tables=tables
        self.com_range=com_range
        self.energy=100
        self.cluster_nodes=[]
        self.active=1
        
    def display(self):
        print("Distance = ",self.distance)
        print("Is cluster head = ",self.isclusterhead)
        print("Tables = ",self.tables)
        print("Communication Range = ",self.com_range)
        print("Energy = ",self.energy)
        if self.isclusterhead:
            print("Nodes belonging to cluster",self.cluster_nodes)

    def match_clus(self,dis):
        if self.distance==dis:
            self.isclusterhead=1
            return True

    def set_cluster(self):
        self.isclusterhead=1

    def set_energy(self,dist):
        self.energy=self.energy-(dist*dist*const)

    def get_energy(self):
        return self.energy

    def get_clus(self):
        return self.isclusterhead

    def get_distance(self):
        return self.distance
    
    def rem_clushead(self):
        self.isclusterhead=0

    def get_tables(self):
        return self.tables

    def get_distance(self):
        return self.distance

    def get_com_range(self):
        return self.com_range

    def set_cluster_nodes(self,index_list):
        self.cluster_nodes=index_list

    def get_cluster_nodes(self):
        return self.cluster_nodes

    def recharge_energy(self):
        self.energy=self.energy+5

    def set_inactive(self):
        self.active=0

    def set_active(self):
        self.active=1

    def get_active(self):
        return self.active
        
        

def node_creator(no):
    n=[]
    distance_list=[]
    node_schemas=[]
    for i in range(no):
        print("Enter co-ordinates for node",i)
        distance=input().strip().split(' ')
        distance=list(map(int,distance))
        #distance_list.append(distance)
        print("Enter communication range for node",i)
        com_range=input().strip().split(' ')
        com_range=list(map(int,com_range))
        print("Enter table schema for node",i)
        tables=input().strip().split(' ')
        #node_schemas.append(tables)
        n.append(Node(distance,tables,com_range[0]))
    #print("Schema list",node_schemas)
    return n
    

def initial_clusterer(n,index):
    temp_max=0
    pos=0
    for i in index:
        if n[i].get_com_range()>temp_max:
            pos=i
            temp_max=n[i].get_com_range()

    n[pos].set_cluster()
    return pos

        




def optimal_clusterer(n,node_list):
    schema_similar_count=0
    temp_max=0
    for i in node_list:
        for j in node_list:
            if j!=i:
                temp_sch=len(list(set(n[j].get_tables()) & set(n[i].get_tables())))
                schema_similar_count+=temp_sch
        print("For node",i)
        print("schema_similar_count",schema_similar_count)
        if schema_similar_count>=temp_max:
            pos=i
            temp_max=schema_similar_count
        schema_similar_count=0

    sum_distance=0
    for i in node_list:
        distance=np.linalg.norm(np.array(n[i].get_distance())-np.array(n[pos].get_distance()))
        sum_distance+=distance

    return (pos,temp_max,sum_distance)

    
    










def final_clusterer(n,index,pos):
    distance_list=[]
    schema_list=[]
    index_list=[]
    com_range=n[pos].get_com_range()
    print("BROADCAST FROM CLUSTER HEAD")
    for i in index:
        distance_list.append(n[i].get_distance())
        schema_list.append(n[i].get_tables())
    print("ACK AND DETAILS FROM REST OF THE NODES") 
    distance_list=[list(map(int,i)) for i in distance_list]
    print("Distance List",distance_list)
    print("Schemas List",schema_list)
    sum_distance=0
    count=0
    schema_similar_count=0
    for i in distance_list:
        distance=np.linalg.norm(np.array(i)-np.array(n[pos].get_distance()))
        #n[count].set_energy(distance)
        #print("count",count)
        if distance<=com_range:
            if index[count]==pos:
                print("Same")
                temp_sch=1
            else:
                temp_sch=len(list(set(n[index[count]].get_tables()) & set(n[pos].get_tables())))
                schema_similar_count+=temp_sch
            n[index[count]].set_energy(distance)
            n[pos].set_energy(distance)
            if temp_sch!=0:
                sum_distance+=distance
                index_list.append(index[count])
        count+=1
    n[pos].set_cluster_nodes(index_list)
    print("sum distances",sum_distance)
    opos,oschema_similar_count,osum_distance=optimal_clusterer(n,index_list)
    print("osum_distance",osum_distance)
    print("schema_similar_count",schema_similar_count)
    print("oschema_similar_count",oschema_similar_count)
    if oschema_similar_count>schema_similar_count and osum_distance<sum_distance:
        print("Removing cluster head status of node",pos)
        n[pos].rem_clushead()
        n[opos].set_cluster()
        print("Adding cluster head status of node",opos)
        n[pos].set_cluster_nodes([])
        n[opos].set_cluster_nodes(index_list)
        distance=np.linalg.norm(np.array(n[opos].get_distance())-np.array(n[pos].get_distance()))
        print("New distance",distance)
        n[pos].set_energy(distance)
    #print(n[pos].get_cluster_nodes())
    return index_list
    



def communication(n):
    print("Enter the site where changes are made")
    site=int(input())
    print("Schemas present at site are :-",*n[site].get_tables())
    print("Enter the columns where changes are made")
    change_list=input().strip().split(' ')
    print(change_list)
    ch_list=[]
    for i in range(len(n)):
        if n[i].get_clus()==1:
            for j in n[i].get_cluster_nodes():
                if j==site:
                    initial_contact_ch=i
                temp_sch=len(list(set(n[j].get_tables()) & set(change_list)))
                if temp_sch!=0:
                    if i not in ch_list:
                        ch_list.append(i)

    
    print("ch_list",ch_list)
    print("initial_contact_ch",initial_contact_ch)
    sum_distance=0
    for i in ch_list:
        for j in n[i].get_cluster_nodes():
            if i!=j:
                temp_sch=len(list(set(n[j].get_tables()) & set(change_list))) 
                if temp_sch!=0:
                    print("Changes made in node",j)
                    distance=np.linalg.norm(np.array(n[i].get_distance())-np.array(n[j].get_distance()))
                    n[j].set_energy(distance)
                    sum_distance+=distance
            if len(n[i].get_cluster_nodes())==1:
                temp_sch=len(list(set(n[j].get_tables()) & set(change_list))) 
                if temp_sch!=0:
                    print("Changes made in node",j)
                    distance=np.linalg.norm(np.array(n[i].get_distance())-np.array(n[j].get_distance()))
                    n[j].set_energy(distance)
                    sum_distance+=distance

        n[i].set_energy(sum_distance)
        distance=np.linalg.norm(np.array(n[i].get_distance())-np.array(n[initial_contact_ch].get_distance()))
        print("Contact from node %d to %d" %(initial_contact_ch,i))
        print("Contact Overhead",distance)
        n[i].set_energy(distance)
        n[initial_contact_ch].set_energy(distance)
        sum_distance=0

    for i in range(len(n)):
        if n[i].get_clus()==1:
            if n[i].get_energy()<=35:
                node_list=n[i].get_cluster_nodes()
                print("Removing cluster head status of node {} as energy low".format(i))
                n[i].rem_clushead()
                n[i].set_cluster_nodes([])
                n[i].set_inactive()
                

                
                already_clustered=[i for i in range(len(n)) if i not in node_list]
                for i in already_clustered:
                    if n[i].get_active()==0:
                        if n[i].get_energy()>=50:
                            n[i].set_active()
                            already_clustered.remove(i)
                    else:
                        if n[i].get_clus()==1:
                            if len(n[i].get_cluster_nodes())<=2:
                                n[i].rem_clushead()
                                for j in n[i].get_cluster_nodes():
                                    already_clustered.remove(j)
                                n[i].set_cluster_nodes([])
                                
                print("Initial already clustered",already_clustered)
                temp=node_list
                f=1
                while f:
                    for i in temp:
                        print("loop temp",temp)
                        print("i",i)
                        if n[i].get_energy()<=35:
                            already_clustered.append(i)
                            temp.remove(i)
                            print("communication temp",temp)
                    ch_pos=initial_clusterer(n,temp)
                    print("ch_pos",ch_pos)
                    temp_c=final_clusterer(n,temp,ch_pos)
                    already_clustered.extend(temp_c)
                    print("already_clustered",already_clustered)
                    temp=[]
                    for j in range(len(n)):
                        if j not in already_clustered:
                            temp.append(j)
                    print("temp",temp)
                    if len(already_clustered)==len(n):
                        break
        
               
                
        
    















def main():
    n=[]
##    print("Enter the number of nodes")
##    no_nodes=int(input())
##    n=node_creator(no_nodes)
##    n.append(Node([50,50],['id','roll','name'],60))                             #0
##    n.append(Node([20,30],['id','roll'],30))                                    #1
##    n.append(Node([100,0],['id','address'],30))                                 #2
##    n.append(Node([30,70],['id','name'],30))                                    #3
##    n.append(Node([40,50],['id','name','roll','marks','section'],40))           #4
##    n.append(Node([70,70],['id','marks','section'],30))                         #5
##    n.append(Node([120,20],['id','lang'],40))                                   #6
##    n.append(Node([100,100],['tid','school'],30))                               #7
##    n.append(Node([110,130],['tid','school','teacher','salary'],60))            #8
##    n.append(Node([120,110],['tid','school','teacher','salary','address'],55))  #9
##    n.append(Node([130,120],['tid','address'],30))                              #10
##    n.append(Node([90,120],['tid','subject'],30))                               #11
##    n.append(Node([30,20],['sid','staff_name'],30))                             #12
    n.append(Node([50,50],['id','roll','name'],150))                             #0
    n.append(Node([20,30],['id','roll'],30))                                    #1
    n.append(Node([100,0],['id','address'],30))                                 #2
    n.append(Node([30,70],['id','name'],30))                                    #3
    n.append(Node([40,50],['id','name','roll','marks','section'],40))           #4
    n.append(Node([70,70],['id','marks','section'],30))                         #5
    n.append(Node([120,20],['id','lang'],100))                                  #6
    n.append(Node([100,100],['tid','school'],30))                               #7
    n.append(Node([110,130],['tid','school','teacher','salary'],150))           #8
    n.append(Node([120,110],['tid','school','teacher','salary','address'],55))  #9
    n.append(Node([130,120],['tid','address'],30))                              #10
    n.append(Node([90,120],['tid','subject'],30))                               #11
    n.append(Node([30,20],['sid','staff_name'],150))                             #12
    f=1
    c=0
    already_clustered=[]
    temp=range(len(n))
    while f:
        ch_pos=initial_clusterer(n,temp)
        print("ch_pos",ch_pos)
        temp_c=final_clusterer(n,temp,ch_pos)
        already_clustered.extend(temp_c)
        print("already_clustered",already_clustered)
        temp=[]
        for j in range(len(n)):
            if j not in already_clustered:
                temp.append(j)
        print("temp",temp)
        c+=len(temp_c)
        if c==len(n):
            f=0
    for i in range(len(n)):
        print("___________________________________________________________")
        print("Details for node ",i)
        n[i].display()
    while True:
        communication(n)
        for i in range(len(n)):
            print("___________________________________________________________")
            print("Details for node ",i)
            n[i].display()
        for i in range(len(n)):
            if n[i].get_energy()<=50:
                print("Increasing Energy of node {} as below 50%".format(i))
                n[i].recharge_energy()
            

        

    
























##    while True:
##        print("Enter 1 for communication 2 for exit")
##        choice=int(input())
##        if choice==2:
##            break
##        centroids=clusterer(distance_list)
##        centroids=[int(i) for i in centroids[0]]
##        print("Cluster Center = ",centroids)
##        distance_list=[list(map(int,i)) for i in distance_list]
##        temp_dist=[]
##        for i in distance_list:
##            distance=np.linalg.norm(np.array(i)-np.array(centroids))
##            temp_dist.append(distance)
##        sum_dist=sum(temp_dist)
##        cluster_head=distance_list[temp_dist.index(min(temp_dist))]
##        print("Cluster Head",cluster_head)
##        temp_dist=[]
##        for i in range(len(n)):
##            t=n[i].match_clus(cluster_head)
##            if t:
##                #print("Node Schema for Cluster Head",node_schemas[i])
##                max_sch=0
##                for j in range(len(node_schemas)):
##                    if j==i:
##                        continue
##                    temp_sch=len(list(set(node_schemas[i]) & set(node_schemas[j])))
##                    print("Count",temp_sch)
##                    
##                    if max_sch<temp_sch:
##                        print("Comming here")
##                        pos=j
##                        max_sch=temp_sch
##                print(pos)
##                if pos==False:
##                    print("Probable cluster",distance_list[pos])
##                    for i in distance_list:
##                        distance=np.linalg.norm(np.array(i)-np.array(distance_list[pos]))
##                        temp_dist.append(distance)
##                    print("sum_dist",sum_dist)
##                    print("sum(temp_dist)",sum(temp_dist))
##                    if sum_dist>sum(temp_dist):
##                        n[pos].match_clus(distance_list[pos])
##                        
##                        
##        for i in range(len(n)):
##            print("======================")
##            print("Details for node ",i)
##            n[i].display()
##            print("======================")
##
##        #communication from cluster head to site 1
##        distance=np.linalg.norm(np.array(distance_list[0])-np.array(cluster_head))
##        print("Distance ",distance)
##
##
##        for i in range(len(n)):
##            if n[i].get_clus()==1:
##                n[i].set_energy(distance) #cluster head energy reduced
##                if n[i].get_energy()<100:
##                    temp=distance_list
##                    temp.remove(n[i].get_distance())
##                    n[i].rem_clushead()
##
##        for i in range(len(n)):
##            print("======================")
##            print("Details for node ",i)
##            n[i].display()
##            print("======================")
##        print("New distances ",temp)
##        centroids=clusterer(temp)
##        centroids=[int(i) for i in centroids[0]]
##        print("Cluster Center = ",centroids)
##        temp=[list(map(int,i)) for i in temp]
##        d=(np.array(temp)-np.array(centroids)).tolist()
##        d=[list(map(abs,i)) for i in d]
##        cluster_head=temp[d.index(min(d))]
##        print("Cluster Head",cluster_head)
##        for i in range(len(n)):
##            n[i].match_clus(cluster_head)
##        for i in range(len(n)):
##            print("======================")
##            print("Details for node ",i)
##            n[i].display()
##            print("======================")
##        distance_list=temp

        
if __name__=="__main__":
    main()
    
