#include<stdio.h>
i#include<stdlib.h>
int main(){
	struct timeval start_time,end_time;
	gettimeofday(&start_time,NULL);
	system("cat outputdata | sort outputdata_new");
	return 0;
	gettimeofday(&end_time,NULL);	
	double data1=(double)start_time.tv_sec+((double)start_time.tv_usec/1000000);
        	           double data2=(double)end_time.tv_sec+((double)end_time.tv_usec/1000000);
       	float dataTime=(double)data2-data1;
	printf("Time Taken %f", dataTime);
}
