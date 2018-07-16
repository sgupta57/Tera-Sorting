// Imports
#include <stdio.h>
#include<stdio.h>
#include<pthread.h>
#include<stdlib.h>
#include<string.h>
#include<time.h>
#include <math.h>
#include <errno.h>

typedef int RC; // Used To give alias to int as RC

// Macros
#define RC_OK 0 
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_READ_FAILED 5
#define PAGE_SIZE 9800
#define FIRST_PAGE 0
#define ONE 1
#define ZERO 0
#define PAGE_OFFSET 1
#define SORT_JOB 0
#define MERGE_JOB 1


typedef int JOB_TYPE;   // Used To give alias

// Variables
char *srcFile = NULL;
char *sortedFile  = NULL;
char *sortDir = "/home/archi/UbuntuComputer/sortJob/";

//char *page = NULL;

// Struct for ??

typedef struct SM_FileHandle {
    char *fileName;
    long totalNumPages;
    long curPagePos;
    void *mgmtInfo;
    FILE *fp;
    int errorNo;
} SM_FileHandle;

// Struct for ??

typedef struct PageInfo{
    unsigned long pageNo;
    unsigned long lineCount;
    char *page;
    char **lines;
    char *outFile;
    PageInfo *next;
}PageInfo;

typedef struct PQueue{
    char *line; 
    PageInfo *pageInfo;
    PageInfo *next;
}PQueue;

PQueue *pQHead =NULL;

PageInfo pageHead=NULL;

static unsigned long JOB_COUNT;     // Variable for counting no of chunks? JOB_COUNT = chunks?

// Struct for ??

typedef struct JobNode{
    unsigned long nodeNumber;
    unsigned long jobNumber;
    int levelNo;
    JOB_TYPE jobType;
    PageInfo *sortPageInfo;
    char *jobFilePath;
}JobNode;



SM_FileHandle sm_fileHandle;        // 
struct LevelNode *mainHead = NULL;  // 
struct JobNode *root = NULL;        // 

unsigned long getLineCount(char* );     //
void printPageLines(char**,  const unsigned long );     //
int partition (char **arr, int low, int high);      //
void sortLines(char **lines, unsigned long count);      //
void quickSort(char **lines, unsigned long left, unsigned long right);  //
void swapLines(char **lines, unsigned long i, unsigned long j);     //
RC createPageFile (SM_FileHandle*);                 //

void swap (const char ** array, long a, long b);    //
void quickSort(char **lines, unsigned long left, unsigned long right);  //



/********************************************************************/

void insertJobNode(PageInfo *, const int , const unsigned long);
void insertLevelNodeHead(const int , JobNode *);
void attacheChildren(JobNode *, const int );
LevelNode* getLevelHeadNode(LevelNode *, const int );
JobNode* getJobNode(JobNode *, const unsigned long );
void printJobListByLevel(const int );
LevelNode* getLastLevelHead();
void createTree(SM_FileHandle *);
/*******************************************************************/

RC processSortJob(JobNode *);
RC *processJob(void *);

/***************************************************************/


int main() {
    char* page;

    char *srcfileName = "/home/archi/UbuntuComputer/Study/Sem-III/CS-553/Assignment/Assign2/pennyinput";
    char *sortFileName = "/home/archi/UbuntuComputer/pennysorted.txt";

    //sm_fileHandle.fileName="/home/archi/UbuntuComputer/Study/Sem-III/CS-553/Assignment/Assign2/pennyinput";

    sm_fileHandle.fileName= srcfileName;

    srcFile = (char *)malloc(sizeof(char) * 200);

    sortedFile = (char *)malloc(sizeof(char) * 200);

    memcpy(srcFile,srcfileName,strlen(srcfileName));
    memcpy(sortedFile,sortFileName,strlen(srcfileName));

    printf("\n Source File Name : %s \n ",srcFile);
    printf("\n Sorted File Name : %s \n ",sortedFile);

    if(openPageFile(&sm_fileHandle)!=RC_OK){
        printf("error in opening file %s ",strerror(errno));
        return 1;
    }

    createTree(&sm_fileHandle);

    createDumySortJob();

  //  createDummyMerge0Job();
    return 0;
}

/******************************************Priority Queue Operation***********************************************/

void insertElementToPQueue(char *line, PageInfo *pageInfo){
    PQueue *element = NULL;
    PQueue *tempEle = NULL; 

    element = (struct PQueue *)malloc(sizeof(struct PQueue *));
    element->pageInfo = pageInfo;

    if(pQHead==NULL){
        pQHead=element
    }else{
        tempEle = pQHead;
        while(tempEle->next!=null){
            if(strncmp(line,tempEle->line) < 0){
                if(tempEle == pQHead){
                    element->next = tempEle;
                    pQHead = element;
                }else{
                    element->next = tempEle;
                    //
                }   

            }
        }
        if(tempEle->next==null){
            tempEle->next = element;
        }
    }

}

/******************************************Create Dummy Job***********************************************/

void createDumySortJob(){
    PageInfo *pageInfo;
    pageInfo = (struct PageInfo*) malloc(sizeof(struct PageInfo*));
    pageInfo->pageNo = 2;

    processJob((void *)pageInfo);
}

void createDummyMerge0Job(){
   
}

/***************************************Create Job Tree***************************************************/


void insertPageElement(const unsigned long pageNo){
    PageInfo *pageInfo,*tempPageInfo; 

    pageInfo = (struct PageInfo *)malloc(sizeof(struct PageInfo *));
    pageInfo->pageNo = pageNo;
    pageInfo->outFile = (char *) malloc (sizeof(char *) * 200);

    memcpy(pageInfo->outFile,sortDir,strlen(sortDir));
    char str[256];
    sprintf(str, "%ld", pageNo);
    strcat(pageInfo->outFile,str);

    if(pageHead==NULL){
        pageHead = pageInfo;
    }else{
        tempPageInfo = pageHead;
        while(tempPageInfo->next!=NULL){
                tempPageInfo = tempPageInfo->next;
        }   
        tempPageInfo->next = pageInfo;
    }
}






void printJobListByLevel(const int level) {
    if (mainHead == NULL)
        return;

    LevelNode *levelHead = getLevelHeadNode(mainHead, level);

    if (levelHead == NULL)
        return;

    JobNode *tempJobNode = levelHead->jobNodeHead;
    do{
        printf("| Job No %ld | ",tempJobNode->jobNumber);
        tempJobNode = tempJobNode->next;
    }while(tempJobNode !=NULL);

}

/****************************************UTIL FUNCTION***************************************************************/

unsigned  long getLineCount(char* page){
    unsigned long lineCount = 0;
    long i=0;
    while(page[i]){
        if(page[i] =='\n'){
            lineCount++;
        }
        i++;
    }
    return lineCount;
}

void printPageLines(char **lines, const unsigned long lineCount){
   unsigned  long int i= 0;
    while(i<lineCount){
        printf("\n Line No :%ld | %s",i,lines[i++]);
    }
}

void getConvertSentences(char **sentence, char *page){

    unsigned long  lc = 0;

    *sentence = (char *)malloc(sizeof(char));
    *sentence = &page[0];

    for(long i = 0 ; i<PAGE_SIZE ; i++){
        if(page[i]=='\n'){
            page[i]='\0';
            if(i!=PAGE_SIZE-1) {
                lc++;
                printf("%d",lc);
                sentence[lc] = (char *)malloc(sizeof(char));
                sentence[lc] = &page[i+1];
            }
        }
    }
}

void convertLinesToPage(char *page){
    for(long i = 0 ; i<PAGE_SIZE; i++){
        if(page[i]=='\0'){
            page[i]='\n';
        }
    }
}


/*********************************************FILE HANDLING**********************************************************/
RC createPageFile (SM_FileHandle *fHandle){
    fHandle->fp = fopen(fHandle->fileName,"w+");
    if (!fHandle->fp){
        fHandle->errorNo =errno;
        return RC_FILE_NOT_FOUND;
    }
    updateSmFileHandle(0,0,fHandle);
    closePageFile(fHandle);
    return RC_OK;
}


RC openPageFile (SM_FileHandle *fHandle){
    fHandle->fp = fopen(fHandle->fileName,"a+");    // rb+
    if (!fHandle->fp){
        return RC_FILE_NOT_FOUND;
    }
    fseek(fHandle->fp,0,SEEK_END);
    unsigned long last_byte_loc = ftell(fHandle->fp);
    int totalNumPages = (int) last_byte_loc / PAGE_SIZE;
    updateSmFileHandle(totalNumPages,0,fHandle);
    rewind(fHandle->fp);
    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle){
    if(fHandle->fp){
        fflush(fHandle->fp);
        if(fclose(fHandle->fp)==ZERO){
            fHandle->errorNo=errno;
            return  RC_OK;
        }
        else
            return RC_FILE_NOT_FOUND;
    }else
        return RC_FILE_HANDLE_NOT_INIT;
}

void updateSmFileHandle(int totalNumPages, int curPagePos,SM_FileHandle *sm_fileHandle){
    sm_fileHandle->totalNumPages = totalNumPages;
    sm_fileHandle->curPagePos = curPagePos;
}


RC readPage (int pageNum, SM_FileHandle *fHandle, char* memPage){   /// memPage is char pointer .. in case get confused
    if(fHandle ==NULL || fHandle->totalNumPages < ONE)
        return RC_FILE_HANDLE_NOT_INIT;
    if(pageNum > fHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;

    //memPage = (char*) malloc(sizeof(char)*PAGE_SIZE);
    fseek(fHandle->fp,PAGE_SIZE * pageNum, SEEK_SET);

    size_t ret_Read = fread(memPage, PAGE_SIZE, sizeof(char),fHandle->fp);
    if(ret_Read <= ZERO)
        return RC_READ_NON_EXISTING_PAGE;
    updateSmFileHandle(fHandle->totalNumPages,pageNum,fHandle);
    return RC_OK;
}

RC getPagePos (SM_FileHandle *fHandle){
    if(fHandle==NULL || fHandle->curPagePos <=ZERO)
        return RC_FILE_HANDLE_NOT_INIT;
    else
        return  fHandle->curPagePos;
}

RC readCurrentPage (SM_FileHandle *fHandle, char* memPage){
    if(fHandle->totalNumPages <=ZERO)
        return RC_FILE_HANDLE_NOT_INIT;
    if(fHandle->fp<=ZERO)
        return RC_FILE_NOT_FOUND;

    int currentPagePos = fHandle->curPagePos;
    memPage =  malloc(sizeof(char)*PAGE_SIZE);
    fseek(fHandle->fp,(PAGE_SIZE * currentPagePos ),SEEK_SET);
    size_t ret_Read=fread(memPage, PAGE_SIZE, sizeof(char),fHandle->fp);

    if(ret_Read <= ZERO)
        return RC_READ_NON_EXISTING_PAGE;

    updateSmFileHandle(fHandle->totalNumPages, currentPagePos,fHandle);

    return RC_OK;
}


RC readNextPage (SM_FileHandle *fHandle, char* memPage) {
    if (fHandle->totalNumPages < FIRST_PAGE)
        return RC_FILE_HANDLE_NOT_INIT;
    if (fHandle->fp == NULL)
        return RC_FILE_NOT_FOUND;
    if (fHandle->curPagePos == (fHandle->totalNumPages-ONE))
        return RC_READ_NON_EXISTING_PAGE;

    int next_Page = fHandle->curPagePos + 1;
    fseek(fHandle->fp, (PAGE_SIZE * next_Page), SEEK_SET);
    memPage = malloc(sizeof(char) * PAGE_SIZE);
    size_t ret_Read = fread(memPage, PAGE_SIZE, sizeof(char),fHandle->fp);
    if (ret_Read <= ZERO)
        return RC_READ_FAILED;
    updateSmFileHandle(fHandle->totalNumPages, next_Page, fHandle);
    return RC_OK;
}


RC writeBlock(int pageNum, SM_FileHandle *fHandle, char* memPage) {
    if (pageNum < 0 || fHandle == NULL) {
        return RC_WRITE_FAILED;
    }
    ensureCapacity(pageNum,fHandle);

    fseek (fHandle->fp, pageNum * PAGE_SIZE, SEEK_SET);
    size_t  rate_out=fwrite (memPage, sizeof(char),PAGE_SIZE,fHandle->fp);

    //fwrite (buff,  sizeof(char),shiftPageBlockSize, fHandle->fp);

    if (rate_out >= PAGE_SIZE) {
        printf("\n Writing to file successful \n");
        updateSmFileHandle((fHandle->totalNumPages + ONE),pageNum,fHandle);
        return RC_OK;
    }else {
        return RC_WRITE_FAILED;
    }

}

RC writeMergePage(int pageNum, SM_FileHandle *fHandle, char* memPage, unsigned long mergePageSize) {
    if (pageNum < 0 || fHandle == NULL) {
        return RC_WRITE_FAILED;
    }
    ensureCapacity(pageNum,fHandle);

    fseek (fHandle->fp, pageNum * PAGE_SIZE, SEEK_SET);
    size_t  rate_out=fwrite (memPage, sizeof(char),mergePageSize,fHandle->fp);

    //fwrite (buff,  sizeof(char),shiftPageBlockSize, fHandle->fp);

    if (rate_out >= PAGE_SIZE) {
        printf("\n Writing to file successful \n");
        updateSmFileHandle((fHandle->totalNumPages + ONE),pageNum,fHandle);
        return RC_OK;
    }else {
        return RC_WRITE_FAILED;
    }

}


RC writeCurrentBlock(SM_FileHandle *fHandle, char* memPage) {
    int pageNum = fHandle->curPagePos;
    if (pageNum < FIRST_PAGE || fHandle->fp == NULL) {
        return RC_WRITE_FAILED;
    }
    char *buff;
    int shiftPageBlockSize = sizeof(char) * (PAGE_SIZE * (fHandle->totalNumPages - pageNum));
    buff = malloc(shiftPageBlockSize);
    fseek (fHandle->fp, pageNum * PAGE_SIZE, SEEK_SET);
    fread (buff,  shiftPageBlockSize,1, fHandle->fp);
    fseek (fHandle->fp, pageNum * PAGE_SIZE, SEEK_SET);
    size_t  rate_out=fwrite (memPage, sizeof(char),PAGE_SIZE,fHandle->fp);
    fwrite (buff,  sizeof(char),shiftPageBlockSize, fHandle->fp);

    if (rate_out >= PAGE_SIZE) {
        updateSmFileHandle((fHandle->totalNumPages + ONE),(pageNum),fHandle);
        return RC_OK;
    }else {
        return RC_WRITE_FAILED;
    }
}

RC ensureCapacity(int expectPageCount, SM_FileHandle *fHandle) {   // this need to be done in loop
    int totalPagesInFile = fHandle->totalNumPages;
    int writesize = ZERO;
    char *write;
    if (expectPageCount > totalPagesInFile) {
        int increasePageCount=(expectPageCount - totalPagesInFile);
        write = (char *) malloc(PAGE_SIZE * sizeof(char));
        memset(write, '\0', PAGE_SIZE);
        fseek(fHandle->fp,0,SEEK_END);
        for (int i = 0; i<increasePageCount; i++){
            writesize = fwrite(write, sizeof(char), PAGE_SIZE, fHandle->fp);
        }

        if (writesize >= 0) {
            updateSmFileHandle((fHandle->totalNumPages + increasePageCount), (fHandle->totalNumPages-ONE + increasePageCount), fHandle);
            return RC_OK;
        } else {
            return RC_WRITE_FAILED;
        }
    }
    return RC_OK;
}
/**************************************JOB PROCESSSING********************************************/

RC *processJob(void *in_jobParam){

    PageInfo *pageInfo = (struct PageInfo*)in_jobParam;

    printf("\n Got Job to process | Page Number :%d   \n",PageInfo->pageNo);


       if( processSortJob(job) !=RC_OK){
            printf("\n Sort Job Failed | Page Number :%d   \n",PageInfo->pageNo);
       }

    return RC_OK;
}


RC processSortJob(PageInfo *sortPageInfo){
    printf("\n  | Starting Sort job | Page No : %ld \n",sortPageInfo->pageNo);

    SM_FileHandle *srcFileHandle = (struct SM_FileHandle *)malloc(sizeof(struct SM_FileHandle));

    SM_FileHandle *sortedFileHandle = (struct SM_FileHandle *)malloc(sizeof(struct SM_FileHandle));

    if(srcFile == NULL || sortedFile == NULL) {
        printf("\n SRC or destination file is not Specified \n ");
        return RC_FILE_NOT_FOUND;
    }
    srcFileHandle->fileName = srcFile;
    sortedFileHandle->fileName = sortPageInfo->outFile;

    if(openPageFile(srcFileHandle)!=RC_OK){
        printf("error in opening file %s ",strerror(errno));
        return 1;
    }
    //char *page_ptr = (char*) malloc(sizeof(char)*PAGE_SIZE);   // changed


    char *page = (char*) malloc(sizeof(char)*PAGE_SIZE);
   // char page[PAGE_SIZE];   // added


    if(readPage(sortPageInfo->pageNo,srcFileHandle,page) != RC_OK){
        printf("error in reading file %s ",strerror(errno));
        closePageFile(srcFileHandle);
        return 1;
    }

  //  memcpy(page,page_ptr,PAGE_SIZE);    // added

  //  free(page_ptr);         // added

    closePageFile(srcFileHandle);

    sortPageInfo->page = page;


    sortPageInfo->lineCount = getLineCount(page);

    char **sentence = (char **)malloc(sizeof(char *)+(sortPageInfo->lineCount)+5);

    printf("\n Page line Count :%ld ",sortPageInfo->lineCount);

    getConvertSentences(sentence,page);

    char **lines = sentence;

    sortPageInfo->lines= lines;

    printPageLines(lines,sortPageInfo->lineCount);

    sortLines(sentence,sortPageInfo->lineCount);

    printf("\n New Line : ");

    printPageLines(lines,sortPageInfo->lineCount);

    convertLinesToPage(page);

    if(createPageFile(sortedFileHandle) != RC_OK){
        printf("error while creating the sorted file");
        return RC_FILE_NOT_FOUND;
    }

    if(openPageFile(sortedFileHandle)!=RC_OK){
        printf("error in opening file %s ",strerror(errno));
        return 1;
    }

    if(writeBlock(sortPageInfo->pageNo,sortedFileHandle,page)!=RC_OK){
        printf("\n Write to sorted file failed ");
    }

    free(page);
    closePageFile(sortedFileHandle);
    free(sortedFileHandle);
    free(srcFileHandle);

    return RC_OK;
}



/*************************************MERGE **************************************************/




RC merge(char** left, char** right, const long leftLineCount, const long rightLineCount, char** a, JobNode *job){
    long ri=0;
    long li=0;
    long j=0;
    unsigned long mergePageCap =  sizeof(char)*PAGE_SIZE * 2;
    unsigned long cur_leftPageProcess = 0;
    unsigned long cur_rightPageProcess = 0;
    char *leftPage = (char*) malloc(sizeof(char)*PAGE_SIZE);
    char *rightPage = (char*) malloc(sizeof(char)*PAGE_SIZE);
    char *mergePage  =  (char*) malloc(mergePageCap + 100);
    char *mergePage_ptr = mergePage;
    char **leftLine;
    char **rightLines;
    PageInfo *leftPageInfo;
    PageInfo *rightPageInfo;
    unsigned long curPageDatSize = 0;
   


    return RC_OK;
}


RC getPageInfo(PageInfo *pageInfo, SM_FileHandle *fileHandle){
    printf("\n Getting data for Page :  %ld",pageInfo->pageNo);
    if(readPage(pageInfo->pageNo,fileHandle,pageInfo->page) != RC_OK){
        printf("error in reading file %s ",strerror(errno));
        closePageFile(fileHandle);
        return 1;
    }
    pageInfo->lineCount = getLineCount(pageInfo->page);

    char **sentence = (char **)malloc(sizeof(char *)+(pageInfo->lineCount)+2);

    printf("\n Page line Count :%ld ",pageInfo->lineCount);

    getConvertSentences(sentence,pageInfo->page);

    char **lines = sentence;

    pageInfo->lines= lines;

    return  RC_OK;
}


/**************************************QUICK SORT********************************************/

void sortLines(char **lines, unsigned long lineCount){
   quickSort(lines, 0, lineCount-1);

  //  quick_sort(lines, 0, lineCount-1);
}

void quickSort(char **lines, unsigned long left, unsigned long right){

    long i = left + 1;
    long j = right;

    if (right-left == 1) {
        if (strncmp (lines[left], lines[right],10) > 0) {
            swapLines (lines, left, right);
        }
        return;
    }

    long mid =(left+right)/2;
    char *pivot =lines[mid];
    swapLines (lines, left, mid);



    while(i <= j) {
        while((i <= right) && (strncmp(lines[i],pivot,10) < 0) ) {

            i = i+1 ;
        }
        while((j >= left) && (strncmp(lines[j],pivot,10) > 0) ) {
            j = j-1;
        }
        if(i < j) {
            swapLines(lines, i, j);
        }
    }

    swapLines (lines, left, j);

    if(left < (j-1)) {
        quickSort(lines, left, j-1);
    }
    if((j+1) < right) {
        quickSort(lines, j+1, right);
    }
}

void swapLines(char **lines, unsigned long i, unsigned long j){
    char * temp;
    temp = lines[i];
    lines[i] = lines[j];
    lines[j] = temp;
}
/**********************************Quick Sort New*****************************************/
