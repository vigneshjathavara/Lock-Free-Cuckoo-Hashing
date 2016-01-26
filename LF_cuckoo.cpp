#include<stdio.h>
#include<stdlib.h>
#include<atomic>
#include<thread>
#include<iostream>
long b = 281474976710656;
#define FIRST 1
#define SECOND 2
#define NIL -1
using namespace std;

class entry
{
	public:
		int key;
		int value;
		entry(int k, int v)
		{
			key = k;
			value = v;
		}
};

int get_cnt(void* ptr){
	unsigned long a;
	int cnt;
	a= ((unsigned long)ptr & (0xffff000000000000));
	cnt= a>>48;
	return cnt;
}


void inc_counter(entry** ptr){
	*ptr = (entry *)((unsigned long)*ptr + b);
}

void store_count(entry** ptr,int cnt){
	unsigned long new_cnt = cnt;
	new_cnt  = new_cnt<<48;
	*ptr = (entry *)((unsigned long)*ptr & 0x0000FFFFFFFFFFFF);
	*ptr = (entry *)((unsigned long)*ptr + new_cnt);
}

entry* extract_address(entry *e){
	e = (entry *)((unsigned long)e & (0x0000fffffffffffc));
	return e;
}


bool is_marked(void *ptr){
	if((unsigned long)ptr & 0x01)
		return true;
	else
		return false;
}

int hashFunc1(int key, int size){
	return key&(size-1);
}

int hashFunc2(int key, int size){
	return key&(size-1);
}

bool checkCounter(int ctr1,int ctr2, int ctrs1, int ctrs2){
	if((ctrs1 - ctr1)>=2 || (ctrs2 - ctr2)>=2)
		return true;
	return false;
}

class cuckooHashTable
{
	public:
		atomic<entry *> *table1;
		atomic<entry *> *table2;
		int t1Size;
		int t2Size;

		cuckooHashTable(int size1, int size2)
		{
			t1Size = size1;
			t2Size = size2;
			table1 = new atomic<entry *>[size1];
			table2 = new atomic<entry *>[size2];
			init();
		}
		void init();
		int Search(int key);
		int Find(int key, entry **e1, entry **e2);
		void Insert(int key, int value);
		void Remove(int key);
		bool Relocate(int tableNum , int pos);
		void help_relocate(int table , int idx, bool initiator);
		void print_table();
		void del_dup(int idx1,entry *e1,int idx2,entry *e2);
};

void cuckooHashTable::init(){
	entry *temp = NULL;
	int i;
	for(i=0;i<t1Size;i++){
		atomic_store( &table1[i], temp);
	}

	for(i=0;i<t2Size;i++)
                atomic_store( &table2[i],temp);
}

void cuckooHashTable::help_relocate(int which , int idx , bool initiator){
	entry *src,*dst,*tmp;
	int dst_idx,nCnt,cnt1,cnt2,size[2];
	atomic<entry *> *tbl[2];
	tbl[0] = table1;
	tbl[1] = table2;
	size[0] = t1Size;
	size[1] = t2Size;
	while(true){
		src =atomic_load_explicit(&tbl[which][idx],memory_order_seq_cst);
		//Marks the entry to logically swap it
		while(initiator && !(is_marked((void *)src)) ){
			tmp = extract_address(src);
			if(tmp == NULL)
				return;
			tmp = (entry *)((unsigned long)src|1);
			atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
			src =atomic_load_explicit(&tbl[which][idx],memory_order_seq_cst);
		}//while

		cnt1 = get_cnt((void *)src);
		if(!(is_marked((void *)src))){
			return;
		}

		dst_idx = hashFunc1(extract_address(src)->key,size[1-which]);
		dst =atomic_load_explicit(&tbl[1-which][dst_idx],memory_order_seq_cst);
		// if dst is null , starts the swap with two CAS
		if(extract_address(dst) == NULL){
			cnt2 =get_cnt((void*)dst);
			nCnt = cnt1>cnt2?cnt1+1:cnt2+1;
			tmp =atomic_load_explicit(&tbl[which][idx],memory_order_seq_cst);

			if(tmp != src){
				continue;
			}

			entry *tmp_src = src;
			tmp_src = (entry *)((unsigned long)src & ~1);
			store_count(&tmp_src,nCnt);
			tmp = NULL;
			store_count(&tmp,cnt1+1);
			if(atomic_compare_exchange_strong(&(tbl[1-which][dst_idx]), &dst, tmp_src))
			atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
			return;
		}//if dst==NULL
		//helper part of the code which helps to finish the second part of the relocate.
		//Might be called by helper thread or in some case by initiator thread also
		if(src == dst){
			tmp = NULL;
			store_count(&tmp,cnt1+1);
			atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
			return;
		}//if src == dst

	tmp = NULL;
	tmp = (entry *)((unsigned long)src&(~1));
	store_count(&tmp,cnt1+1);
	atomic_compare_exchange_strong(&(tbl[which][idx]), &src, tmp);
	return;

}
}

void cuckooHashTable::del_dup(int idx1,entry *e1,int idx2,entry *e2){
	entry *tmp1,*tmp2;
	int key1,key2,cnt;
		tmp1 = atomic_load(&table1[idx1]);
		tmp2 = atomic_load(&table2[idx2]);
		if((e1 != tmp1)&&(e2 != tmp2))
			return;
		key1 = extract_address(e1)->key;
		key2 = extract_address(e2)->key;
		if(key1 != key2)
			return;
	tmp2 = NULL;
	cnt = get_cnt(e2);
	store_count(&tmp2,cnt+1);
	atomic_compare_exchange_strong(&(table2[idx2]), &e2, tmp2);
}

void cuckooHashTable::print_table()
{
	printf("******************hash_table 1*****************\n");
	int i;
	entry *e,*tmp = NULL;
	for(i=0;i<t1Size;i++){
		if(table1[i] != NULL){
			e = atomic_load_explicit(&table1[i],memory_order_relaxed);
			tmp = extract_address(e);
			if(tmp != NULL)
			printf("%d\t%016lx\t%d\t%d\n",i,(long)e ,tmp->key,tmp->value);
			else
			printf("%d\t%016lx\n",i,(long)e);
				}
		else
			printf("%d\tNULL\n",i);
	}
	printf("****************hash_table 2*******************\n");
	for(i=0;i<t2Size;i++){
		if(table2[i] != NULL){
			e = atomic_load_explicit(&table2[i],memory_order_relaxed);
			tmp = extract_address(e);
			if(tmp != NULL)
			printf("%d\t%016lx\t%d\t%d\n",i,(long)e ,tmp->key,tmp->value);
			else
			printf("%d\t%016lx\n",i,(long)e);
				}
		else
			printf("%d\tNULL\n",i);
	}
	printf("\n");


}

int cuckooHashTable::Search(int key)
{
	int h1,h2;
	int c1,c2,c1s,c2s;
	while(1){
		h1 = hashFunc1(key,t1Size);
		h2 = hashFunc2(key,t2Size);

		entry *e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		c1 = get_cnt(e);
		e= extract_address(e);
		//Looking in table 1
		if(e!= NULL && e->key == key)
			return e->value;
		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		c2 = get_cnt(e);
		e= extract_address(e);
		//Looking in table 2
		if(e!= NULL && e->key == key)
			return e->value;

		//second round
		e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		c1s = get_cnt(e);
		e= extract_address(e);

		if(e!= NULL && e->key == key)
			return e->value;
		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		c2s = get_cnt(e);
		e= extract_address(e);
		if(e!= NULL && e->key == key)
			return e->value;

		if(checkCounter(c1,c2,c1s,c2s))
			continue;
		return NIL;
	}
}

int cuckooHashTable::Find(int key, entry **e1, entry **e2)
{
	int h1,h2,result=0;
	int c1,c2,c1s,c2s;
	while(1){
		h1 = hashFunc1(key,t1Size);
		h2 = hashFunc2(key,t2Size);
		/*********************Round 1************************************/
		entry *e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		*e1=e;
		c1 = get_cnt(e);
		e= extract_address(e);
		// helping other concurrent thread if its not completely done with its job
		if(e != NULL){
			if(is_marked((void*)e))	{
			help_relocate(0,h1,false);
			continue;
			}
			else if(e->key == key)
				result = FIRST;
		}

		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		*e2=e;
		e= extract_address(e);
		c2 = get_cnt(e);
		e= extract_address(e);
		// helping other concurrent thread if its not completely done with its job
		if(e != NULL){
			if(is_marked((void*)e))	{
				help_relocate(1,h2,false);
				continue;
			}
			if(e->key == key ){
				if( result == FIRST){
					printf("Find(): Delete_dup()\n");
					del_dup(h1,*e1,h2,*e2);
				}
				else
			   		result = SECOND;
			}
		}

		if(result == FIRST || result == SECOND)
			return result;
		/*********************Round 2************************************/
		e = atomic_load_explicit(&table1[h1],memory_order_relaxed);
		*e1=e;
		c1s = get_cnt(e);
		e= extract_address(e);
		if(e != NULL){
			if(is_marked((void*)e))	{
				help_relocate(0,h1,false);
				printf("Find(): help_relocate()");
				continue;
			}
			else if(e->key == key)
				result = FIRST;
		}

		e = atomic_load_explicit(&table2[h2],memory_order_relaxed);
		*e2=e;
		e= extract_address(e);
		c2s = get_cnt(e);
		e= extract_address(e);
		if(e != NULL){
			if(is_marked((void*)e))	{
					help_relocate(1,h2,false);
					continue;
			}
			if(e->key == key ){
				if( result == FIRST){
					printf("Find(): Delete_dup()\n");
					del_dup(h1,*e1,h2,*e2);
				}
				else
			    	result = SECOND;
			}
		}

		if(result == FIRST || result == SECOND)
			return result;
		if(checkCounter(c1,c2,c1s,c2s)){
			continue;
		}
		return NIL;
	}//end while(1)
}


void cuckooHashTable::Insert(int key , int value)
{
	entry *newEntry = new entry(key,value);
	entry *ent1 = NULL, *ent2 = NULL;
	int cnt=0,h1,h2;
	h1 = hashFunc1(key,t1Size);
	h2 = hashFunc2(key,t2Size);

	while(true)
	{
		int result = Find(key, &ent1, &ent2);
		//updating existing content
		if(result == 1)
		{
			cnt = get_cnt(ent1);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
			delete[] extract_address(ent1);
			if(casResult == true)
				return;
			else
				continue;
		}

		if(result == 2)
		{
			cnt = get_cnt(ent2);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
			delete[] extract_address(ent2);
			if(casResult == true)
				return;
			else
				continue;
		}
		//avoiding double duplicate instance of key
		if(extract_address(ent1) == NULL && extract_address(ent2) == NULL)
				{
					cnt = get_cnt(ent2);
					store_count(&newEntry,cnt+1);
					bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
					if(casResult == true)
						return;
					else{
						continue;
					}
				}
		if(extract_address(ent1) == NULL)
		{
			cnt = get_cnt(ent1);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
			delete[] extract_address(ent1);
			if(casResult == true)
				return;
			else
				continue;
		}

		if(extract_address(ent2) == NULL)
		{
			cnt = get_cnt(ent2);
			store_count(&newEntry,cnt+1);
			bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
			delete[] extract_address(ent2);
			if(casResult == true)
				return;
			else
				continue;
		}

		bool relocateResult = cuckooHashTable::Relocate(1,hashFunc1(key,t1Size));
		if(relocateResult == true){
			continue;
		}
		else{
			//rehash
			return;
		}
	}
}



void cuckooHashTable::Remove(int key)
{
    entry *ent1 = NULL, *ent2 = NULL;
  	int cnt=0,h1,h2;
  	h1 = hashFunc1(key,t1Size);
  	h2 = hashFunc2(key,t2Size);
          while(true){
                  int result = Find(key, &ent1, &ent2);
                  if(result == FIRST){
                	  	  entry *tmp = NULL;
				   	  	  cnt = get_cnt(ent1);
                	  	  store_count(&tmp,cnt+1);
                	  	  bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, tmp);
                	  	  if(casResult == true){
                	  		  return;
                	  	  }
                	  	  else
                	  		  continue;
                  	  }
					else if(result == SECOND){
							if(table1[h1] != ent1){
								continue;
							}
							entry *tmp = NULL;
							cnt = get_cnt(ent2);
							store_count(&tmp,cnt+1);
							bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, tmp);
							if(casResult == true){
								return;
							}
							else
								continue;
					}

					else
						return;
          }
}


bool cuckooHashTable::Relocate(int tableNum, int pos)
{
	int threshold = t1Size+t2Size;
	int route[threshold];
	int start_level = 0, tbl_num = tableNum, idx = pos, pre_idx=0,key;
	atomic<entry *> *tbl[2];
		tbl[0] = table1;
		tbl[1] = table2;
	path_discovery:
	bool found = false;
	int depth = start_level;

	do{
		entry *e1 = NULL;
		entry *pre = NULL;
		e1 = atomic_load(&tbl[tbl_num][idx]);
		while( is_marked((void *)e1) ){
			help_relocate(tbl_num,idx,false);
			e1 = atomic_load(&tbl[tbl_num][idx]);
		}
		if (depth >0){
			entry *pre_addr,*e1_addr;//assign to masked value
			pre_addr = extract_address(pre);
			e1_addr = extract_address(e1);
			if(e1_addr != NULL && pre_addr != NULL){
				if(pre == e1 || pre_addr->key == e1_addr->key){
					if(tbl_num == 0)
						del_dup(idx,e1,pre_idx,pre);
					else
						del_dup(pre_idx,pre,idx,e1);
				}
			}
		}

		if(extract_address(e1) != NULL){
			route[depth] = idx;
			key = extract_address(e1)->key;
			pre = e1;
			pre_idx = idx;
			tbl_num = 1-tbl_num;
			idx =  (tbl_num == 0 ? hashFunc2(key,t1Size) : hashFunc1(key,t2Size));
		}

		else{
			found = true;
		}
	}while(!found && ++depth<threshold);

	if(found){
		entry *e,*dst;
		int dst_idx,key;
		tbl_num = 1-tbl_num;
		for(int i=depth-1; i>=0; --i, tbl_num = 1-tbl_num){
			idx = route[i];
			e = atomic_load(&tbl[tbl_num][idx]);
			if(is_marked((void *)e)){
					help_relocate(tbl_num,idx,false);
					e = atomic_load(&tbl[tbl_num][idx]);
			}
			if(extract_address(e) == NULL){
					continue;
			}
			key = extract_address(e)->key;
			if(tbl_num == 0)
				dst_idx = hashFunc2(key,t2Size);
			else
				dst_idx = hashFunc1(key,t1Size);
			dst = atomic_load(&tbl[1-tbl_num][dst_idx]);
			if(extract_address(dst) != NULL){
				start_level=i+1;
				idx = dst_idx;
				tbl_num = 1-tbl_num;
				goto path_discovery;
			}
			help_relocate(tbl_num,idx,true);
		}
	}
	return found;
}
class cuckooHashTable myHash(8,16);
void* test1(void*){
	//x->print_table();
	//insert to pos null table1
	cuckooHashTable *x = &myHash;
	x->Insert(13,10);
	x->Insert(21,10);
	//test  strategy
	x->print_table();
	x->Insert(37,123);
	x->Insert(53,1);//testing rehash
	x->print_table();
	printf("x->Search(37) - %d\n",x->Search(37));
	printf("Table after insert\n");
	x->Remove(37);
	x->Insert(13,1234);
	printf("x->Search(37) - %d\n",x->Search(37));
	x->Insert(9,345);
	x->Insert(1,1);
	x->Insert(17,2);
	x->print_table();
}
int main()
{

	int rc,i;
	pthread_t threads[2];

		 cout <<"main() : creating thread,1 "<< endl;
		rc = pthread_create(&threads[0], NULL,test1, NULL);

		 cout <<"main() : creating thread,2 "<< endl;
		rc = pthread_create(&threads[1], NULL,test1, NULL);
	      if (rc){
	         cout << "Error:unable to create thread," << rc << endl;
	         exit(-1);
	      }


	pthread_join(threads[0],NULL);
	pthread_join(threads[1],NULL);
	printf("End\n");
	return 1;
}

