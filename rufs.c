/*
 *  Copyright (C) 2022 CS416/518 Rutgers CS
 *	RU File System
 *	File:	rufs.c
 *
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>

#include "block.h"
#include "rufs.h"

#define TOTAL_BLOCKS (DISK_SIZE/BLOCK_SIZE)
#define FILE_TYPE 1
#define DIR_TYPE 2

char diskfile_path[PATH_MAX];
FILE* disk;

// Declare your in-memory data structures here
struct superblock* super;
struct inode* rootNode;
struct dirent* rootDir;
struct inode* inode; //generic inode
char BUFFER[BLOCK_SIZE];
bitmap_t inodeBits;
bitmap_t dataBits;
uint32_t nodesPerBlock;
uint32_t inodeBlocks;
_Bool opened = 0;

/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {
	printf("Looking for available inodes\n");
	//inode of zero is NULL
	bio_read(1, inodeBits);
	int available = -1;
	// Step 1: Read inode bitmap from disk
	for(int i=0; i<MAX_INUM; i++){
		if(!get_bitmap(inodeBits, i)){
			available = i;
			break;
		}
	}
	if(available == -1){
		printf("Out of inodes\n");
		exit(1);
	}
	// Step 2: Traverse inode bitmap to find an available slot
	set_bitmap(inodeBits, available);
	// Step 3: Update inode bitmap and write to disk 
	bio_write(1, inodeBits);
	printf("Found inode: %d\n", available+1);
	return available+1;
}

/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {
	printf("Looking for available blocks\n");
	bio_read(2, dataBits);
	int available = -1;
	// Step 1: Read data block bitmap from disk
	for(int i=0; i<MAX_DNUM; i++){
		if(!get_bitmap(dataBits, i)){
			available = i;
			break;
		}
	}
	if(available == -1){
		printf("Out of data\n");
		exit(1);
	}
	// Step 2: Traverse data block bitmap to find an available slot
	set_bitmap(dataBits, available);
	// Step 3: Update data block bitmap and write to disk 
	bio_write(2, dataBits);
	printf("Found data: %d\n", available+1);
	return available+1;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {
	uint16_t blk_num = ((ino-1) / nodesPerBlock) + 3;
	uint16_t offset = (ino-1) % nodesPerBlock;
	bio_read(blk_num, BUFFER);
	memcpy(inode, BUFFER+(offset*sizeof(struct inode)), sizeof(struct inode));
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {
	uint16_t blk_num = ((ino-1) / nodesPerBlock) + 3;
	uint16_t offset = (ino-1) % nodesPerBlock;
	bio_read(blk_num, BUFFER);
	memcpy(BUFFER+(offset*sizeof(struct inode)), inode, sizeof(struct inode));
	bio_write(blk_num, BUFFER);
	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {
	printf("Find Dir(ino = %d, fname = %s)\n", ino, fname);
	struct inode* dirNode = new_inode();
	struct inode* fileNode;
	readi(ino, dirNode);
	int dir_ptr;
	struct dirent* blk_dir = (struct dirent*)malloc(BLOCK_SIZE);
	int blk_num;
	//go through direct pointers
	for(int i=0; i<16; i++){
		dir_ptr = dirNode->direct_ptr[i] - 1;
		blk_num = dir_ptr + super->d_start_blk;
		if(dirNode->direct_ptr[i] == 0 || !get_bitmap(dataBits, dir_ptr)) continue; //invalid file
		bio_read(blk_num, blk_dir);
		//go through dirents in the block
		for(int j=0; j<(BLOCK_SIZE/sizeof(struct dirent)); j++){
			memcpy(dirent, blk_dir+j, sizeof(struct dirent));
			if(dirent->valid && !strcmp(fname, dirent->name)){
				fileNode = new_inode();
				readi(dirent->ino, fileNode);
				//dir has been found
				free(dirNode);
				free(fileNode);
				free(blk_dir);
				printf("File %s found in %d\n", fname, ino);
				return 0;
			}
		}
	}	
	printf("File Not Found in Dir\n");
	free(blk_dir);
	free(dirNode);
	return -1;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	printf("Add File to Dir (dir_ino = %d, f_ino = %d, fname = %s)\n", dir_inode.ino, f_ino, fname);
	int dir_ptr;
	int blk_num;
	char* blk_dir = malloc(BLOCK_SIZE);
	int empty_blk = -999; //index of the first empty block to be written
	int empty_entry = -999; //index of the first empty entry within a block;
	int empty_entry_blk = -999; //index of first empty entry within a nonempty block;
	struct dirent* empty_dirent = NULL;
	
	//Does the file already exist?
	struct dirent* dirent = new_dirent();
	if(!dir_find(dir_inode.ino, fname, name_len, dirent)){
		//yes
		printf("File already exists\n");
		return -1;
	}

	//iterating through all direct ptrs
	for(int i=0; i<16; i++){
		dir_ptr = dir_inode.direct_ptr[i] - 1;
		blk_num = dir_ptr + super->d_start_blk;
		//do we have an empty dirent yet?
		if(empty_dirent != NULL) break;
		if(dir_inode.direct_ptr[i] == 0 || !get_bitmap(dataBits, dir_ptr)){
			empty_blk = ((empty_blk == -999) ? i : empty_blk);
			continue;
		}
		bio_read(blk_num, blk_dir);
		//load all dirents into blk_dir
		for(int j=0; j<(BLOCK_SIZE/sizeof(struct dirent)); j++){
			memcpy(dirent, blk_dir+(j*sizeof(struct dirent)), sizeof(struct dirent));
			if(!dirent->valid){
				//found the dirent to write into
				empty_dirent = dirent;
				empty_entry = j;
				empty_entry_blk = i;
				break;
			}
		}
	}
	printf("Empty blk = %d\nEmpty entry = %d\nEmpty entry blk = %d\n", empty_blk, empty_entry, empty_entry_blk);

	//is dir full?
	if(empty_dirent == NULL && empty_blk == -999){
		free(blk_dir);
		free(dirent);
		printf("Dir is Full\n");
		return -1;
	}
	else if(empty_dirent == NULL){
		//block is all used up, need a new one
		printf("Need new block\n");
		dir_inode.direct_ptr[empty_blk] = get_avail_blkno();
		blk_num = (dir_inode.direct_ptr[empty_blk]) - 1 + super->d_start_blk;
		bio_read(blk_num, blk_dir);
		//new entry for a new block
		empty_dirent = new_dirent();
		empty_dirent->ino = f_ino;
		empty_dirent->valid = 1;
		strncpy(empty_dirent->name, fname, name_len);
		//add back into data block
		memcpy(blk_dir, empty_dirent, sizeof(struct dirent));
		bio_write(blk_num, blk_dir);
		writei(dir_inode.ino, &dir_inode);
	} else {
		//some space left over
		printf("Some space left in this block\n");
		blk_num = (dir_inode.direct_ptr[empty_entry_blk]) - 1 + super->d_start_blk;
		bio_read(blk_num, blk_dir);
		empty_dirent->ino = f_ino;
		empty_dirent->valid = 1;
		strncpy(empty_dirent->name, fname, name_len);
		memcpy(blk_dir+(empty_entry*sizeof(struct dirent)), empty_dirent, sizeof(struct dirent));
		bio_write(blk_num, blk_dir);
		writei(dir_inode.ino, &dir_inode);
	}
	printf("File %s with ino %d added\n", fname, f_ino);
	return 0;
}

int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {
	printf("Remove from dir (dir_ino = %d, fname = %s)\n", dir_inode.ino, fname);
	int ptr, blk_num;
	struct dirent* blk_dir = (struct dirent*)malloc(BLOCK_SIZE);
	struct dirent* dirent = new_dirent();
	for(int i=0; i<16; i++){
		//valid dir?
		ptr = dir_inode.direct_ptr[i] - 1;
		blk_num = ptr + super->d_start_blk;
		if(!dir_inode.direct_ptr[i] || !get_bitmap(dataBits, ptr)) continue;
		
		bio_read(blk_num, blk_dir);
		for(int j=0; j<(BLOCK_SIZE / sizeof(struct dirent)); j++){
			memcpy(dirent, blk_dir+j, sizeof(struct dirent));
			//is this the right entry?
			if(dirent->valid && !strcmp(fname, dirent->name)){
				//yes
				dirent->valid = 0;
				memcpy(blk_dir+j, dirent, sizeof(struct dirent));
				bio_write(blk_num, blk_dir);
				//file-be-gone
				free(blk_dir);
				free(dirent);
				printf("File %s deleted from dir %d\n", fname, dir_inode.ino);
				return 0;
			}
		}
	}
	printf("File not found\nCannot Delete\n");
	free(blk_dir);
	free(dirent);
	return -1;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
	printf("Getting node by path (path = %s, ino = %d)\n", path, ino);
	//get names out of path
	char* base = strdup(path);
	char* point;
	_Bool found = 0;
	struct dirent* nextDir = new_dirent();
	while(!found){
		point = strtok_r(base, "/", &base);
		if(point == NULL){
			//we found terminal point
			readi(ino, inode);
			printf("Path %s has inode %d\n", path, ino);
			return 0;
		}
		if(!dir_find(ino, point, strlen(point), nextDir)){
			ino = nextDir->ino;
			continue;
		}
		else break;
	}
	printf("Didn't get node by path\n");
	return 1;
}

/* 
 * Make file system
 */
int rufs_mkfs() {

	printf("Making fs\n");
	// Call dev_init() to initialize (Create) Diskfile
	dev_init(diskfile_path);
	opened = 1;
	super = (struct superblock*)malloc(sizeof(struct superblock));

	// write superblock information
	super->magic_num = MAGIC_NUM;
	super->max_inum = MAX_INUM;
	super->max_dnum = MAX_DNUM;
	super->i_bitmap_blk = 1 * BLOCK_SIZE;
	super->d_bitmap_blk = 2 * BLOCK_SIZE;
	super->i_start_blk = 3 * BLOCK_SIZE;
	nodesPerBlock = BLOCK_SIZE / sizeof(struct inode);
	inodeBlocks = (MAX_INUM * sizeof(struct inode)) / BLOCK_SIZE;
	super->d_start_blk = 3+inodeBlocks;
	bio_write(0, super);

	// initialize inode bitmap
	inodeBits = (bitmap_t)malloc(BLOCK_SIZE);
	memset(inodeBits, '0', BLOCK_SIZE);
	bio_write(1, inodeBits);
	
	// initialize data block bitmap
	dataBits = (bitmap_t)malloc(BLOCK_SIZE);
	memset(dataBits, '0', BLOCK_SIZE);
	bio_write(2, dataBits);
	
	// update bitmap information for root directory
	set_bitmap(inodeBits,0);
	set_bitmap(dataBits,0);

	//setting up root inode
	rootNode = new_inode();
	rootNode->ino = get_avail_ino();
	rootNode->valid = 1;
	rootNode->size = 0;
	rootNode->type = DIR_TYPE;
	rootNode->link = 2;
	rootNode->direct_ptr[0] = get_avail_blkno();

	writei(rootNode->ino, rootNode);

	printf("FS made\n");
	return 0;
}


/* 
 * FUSE file operations
 */
static void *rufs_init(struct fuse_conn_info *conn) {
	printf("Initializing FS\n");
	if(!opened) rufs_mkfs();
	else{
		if(dev_open(diskfile_path) == -1){
			printf("Couldn't open Disk File\n");
			exit(1);
		}
		//init data structures
		
		bio_read(0, BUFFER);
		memcpy(super, BUFFER, sizeof(struct superblock));

		inodeBits = (bitmap_t)malloc(BLOCK_SIZE);
		dataBits = (bitmap_t)malloc(BLOCK_SIZE);
		printf("FS initialized\n");
	}
	return NULL;
}

static void rufs_destroy(void *userdata) {
	printf("Destroying FS\n");
	free(super);
	free(inodeBits);
	free(dataBits);
	dev_close(diskfile_path);
	printf("FS Destroyed\n");
}

static int rufs_getattr(const char *path, struct stat *stbuf) {
	printf("Getting attributes\n");
	inode = new_inode();
	//can we get the file at the path?
	if(get_node_by_path(path, 1, inode)){
		//no
		free(inode);
		printf("Attributes Don't Exist, File Doesn't Exist\n");
		return -ENOENT;
	}
	//yes, time to get the stats
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	stbuf->st_ino = inode->ino;
	stbuf->st_nlink = inode->link;
	stbuf->st_size = inode->size;
	stbuf->st_blksize = BLOCK_SIZE;
	stbuf->st_mode = (inode->type == FILE_TYPE) ? (S_IFREG | 0666) : (S_IFDIR | 0755);
	stbuf->st_atime = time(NULL);
	stbuf->st_mtime = time(NULL);
	//stats done
	free(inode);
	printf("Attributes gotten\n");
	return 0;
}

static int rufs_opendir(const char *path, struct fuse_file_info *fi) {
	printf("Opening dir\n");
	inode = new_inode();
	get_node_by_path(path, 1, inode);
	if(inode->type != DIR_TYPE){
		//this is a file
		free(inode);
		printf("Not a dir to be opened\n");
		return -1;
	}
	//this is a dir
	free(inode);
	printf("Opened dir\n");
    return 0;
}

static int rufs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	printf("Reading dir\n");
	filler(buffer, ".", NULL, 0);
	filler(buffer, "..", NULL, 0);
	inode = new_inode();
	//can we get file at path?
	if(get_node_by_path(path, 1, inode)){
		//no
		free(inode);
		printf("Cannot be read\n");
		return -ENOENT;
	}
	//yes, but is this a dir?
	if(inode->type != DIR_TYPE){
		//no
		free(inode);
		printf("Passed in a file instead of a dir\n");
		return -1;
	}
	//yes, time to read em all out

	struct dirent* dirent = new_dirent();
	int ptr, blk_num;
	for(int i=0; i<16; i++){
		ptr = inode->direct_ptr[i] -1;
		blk_num = ptr + super->d_start_blk;
		if(!inode->direct_ptr[i] || !get_bitmap(dataBits, ptr)) continue;
		bio_read(blk_num, buffer);
		for(int j=0; j < (BLOCK_SIZE / sizeof(struct dirent)); j++){
			//get the current dir
			memcpy(dirent, buffer+(j*sizeof(struct dirent)), sizeof(struct dirent));
			//dir is invalid
			if(!dirent->valid) filler(buffer, dirent->name, NULL, 0);
		}
	}
	free(dirent);
	printf("Dir read\n");
	return 0;
}


static int rufs_mkdir(const char *path, mode_t mode) {
	printf("MKDIR (path = %s)\n", path);
	char* dir_name = dirname(strdup(path)), *base_name = basename(strdup(path));
	inode = new_inode();
	if(get_node_by_path(dir_name, 1, inode)){
		//dir doesn't exist
		free(inode);
		printf("Original Dir doesn't exist\n");
		return -ENOENT;
	}

	int newIno = get_avail_ino();
	struct inode* subDirNode = new_inode();
	readi(newIno, subDirNode);
	subDirNode->ino = newIno;
	subDirNode->valid = 1;
	subDirNode->size = 0;
	subDirNode->type = DIR_TYPE;
	subDirNode->link = 2;
	for(int i=0; i<16; i++){
		//setting up ptrs for new dir
		subDirNode->direct_ptr[i] = 0;
		if(i<8) subDirNode->indirect_ptr[i] = 0;
	}
	subDirNode->vstat.st_atime = time(NULL);
	subDirNode->vstat.st_mtime = time(NULL);
	writei(newIno, subDirNode);
	dir_add(*inode, newIno, base_name, strlen(base_name));
	printf("Dir created\n");
	return 0;
}

static int rufs_rmdir(const char *path) {
	printf("RMDIR (path = %s)\n", path);
	if(!strcmp(path, "/")){
		printf("Trying to delete root\n");
		return -EPERM;
	}
	char* dir_name = dirname(strdup(path)), *base_name = basename(strdup(path));
	inode = new_inode();
	if(get_node_by_path(path, 1, inode)){
		//specified path doesn't exist
		printf("Trying to delete a nonexistent dir\n");
		return -ENOENT;
	}
	if(inode->type != DIR_TYPE){
		printf("Not a dir to delete\n");
		return -ENOENT;
	}

	//clear out all files inside of dir
	int ptr, blk_num;
	for(int i=0; i<16; i++){
		ptr = inode->direct_ptr[i];
		if(!ptr) continue;
		blk_num = (ptr -1) + super->d_start_blk;
		bio_read(blk_num, BUFFER);
		memset(BUFFER, 0, BLOCK_SIZE);
		bio_write(blk_num, BUFFER);
		unset_bitmap(dataBits, ptr-1);
		inode->direct_ptr[i] = 0;
	}
	inode->valid = 0;
	inode->type = 0;
	inode->link = 0;
	writei(inode->ino, inode);

	//remove dir itself
	unset_bitmap(inodeBits, (inode->ino)-1);
	get_node_by_path(dir_name, 1, inode);
	dir_remove(*inode, base_name, strlen(base_name));
	free(inode);
	printf("Dir removed\n");
	return 0;
}

static int rufs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	printf("CREATE (path = %s)\n", path);
	//essentially same as mkdir
	char* dir_name = dirname(strdup(path)), *base_name = basename(strdup(path));
	inode = new_inode();
	if(get_node_by_path(dir_name, 1, inode)){
		//dir doesn't exist
		printf("Cannot create file\n");
		free(inode);
		return -ENOENT;
	}

	int newIno = get_avail_ino();
	struct inode* fileNode = new_inode();
	readi(newIno, fileNode);
	fileNode->ino = newIno;
	fileNode->valid = 1;
	fileNode->size = 0;
	fileNode->type = FILE_TYPE;
	fileNode->link = 1;
	for(int i=0; i<16; i++){
		//setting up ptrs for new dir
		fileNode->direct_ptr[i] = 0;
		if(i<8) fileNode->indirect_ptr[i] = 0;
	}
	fileNode->vstat.st_atime = time(NULL);
	fileNode->vstat.st_mtime = time(NULL);
	writei(newIno, fileNode);
	dir_add(*inode, newIno, base_name, strlen(base_name));
	printf("File created\n");
	return 0;
}

static int rufs_open(const char *path, struct fuse_file_info *fi) {
	printf("Opening File\n");
	inode = new_inode();
	if(get_node_by_path(path, 1, inode)){
		printf("File doesn't exist\nCannot open\n");
		free(inode);
		return -1;
	}
	printf("File Opened\n");
	return 0;
}

static int rufs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	printf("READ (path = %s, size = %lu, offset = %lu)\n", path, size, offset);
	inode = new_inode();
	if(get_node_by_path(path, 1, inode)){
		printf("File doesn't exist\nCannot be read\n");
		return -ENOENT;
	}

	//part 1: direct ptrs
	int b_copy = 0, b_read = 0;
	int blk_ind = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE, blk_num;
	char*  d_blk_dir = malloc(BLOCK_SIZE);
	while(b_copy < size || blk_ind >= 16){
		//valid ptr?
		if(inode->direct_ptr[blk_ind] == 0) break;
		blk_num = (inode->direct_ptr[blk_ind] - 1) + super->d_start_blk;
		bio_read(blk_num, d_blk_dir);
		b_read = ((size - b_copy) > BLOCK_SIZE) ? BLOCK_SIZE : size - b_copy;
		memcpy(buffer+b_copy, d_blk_dir+blk_offset, b_read);
		b_copy += b_read;
		blk_offset = 0;
		blk_ind++;
	}

	//part 2: indirect ptrs
	char* i_blk_dir = malloc(BLOCK_SIZE);
	int indirect_index = 0, i_start, i_blk_offset;
	if(b_copy == 0){
		i_start = offset - 16*BLOCK_SIZE;
		i_blk_offset = i_start / BLOCK_SIZE;
		indirect_index = i_blk_offset / (4*BLOCK_SIZE);
		i_start = i_blk_offset % (indirect_index * 4 * BLOCK_SIZE);
	}
	int blk_id;
	while(b_copy < size){
		if(inode->indirect_ptr[indirect_index] == 0) break;
		blk_num = (inode->indirect_ptr[indirect_index] - 1) + super->d_start_blk;
		bio_read(blk_num, i_blk_dir);
		for(int i=i_start; i<BLOCK_SIZE; i += 4){
			blk_id = (*((int*)(i_blk_dir+i))) - 1;
			blk_id += super->d_start_blk;
			if(get_bitmap(dataBits, blk_id)){
				bio_read(blk_id, d_blk_dir);
				b_read = (size - b_copy > BLOCK_SIZE) ? BLOCK_SIZE : size - b_copy;
				memcpy(buffer+b_copy, d_blk_dir+blk_offset, b_read);
				b_copy += b_read;
				blk_offset = 0;
			}
		}
		i_start = 0;
		indirect_index++;
	}

	free(inode);
	free(d_blk_dir);
	free(i_blk_dir);
	printf("File read\n");
	return b_copy;
}

static int rufs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	printf("WRITE (path = %s, size = %lu, offset = %lu)\n", path, size, offset);
   	inode = new_inode();
	if(get_node_by_path(path, rootNode->ino, inode)) {
		printf("File cannot be written\n");
		return -ENOENT;
	}
	size_t currentSize = inode->size;
    int b_write = 0, b_done = 0;
	int blk_ind = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE, blk_num;
	char* blk_dir = malloc(BLOCK_SIZE);

	//part 1: direct ptrs
	while(b_done < size){
		if(inode->direct_ptr[blk_ind] == 0) inode->direct_ptr[blk_ind] = get_avail_blkno();
		blk_num = (inode->direct_ptr[blk_ind]) - 1 + super->d_start_blk;
		bio_read(blk_num, blk_dir);
		b_write = (size - b_done > BLOCK_SIZE) ? BLOCK_SIZE : size - b_done;
		memcpy(blk_dir+blk_offset, buffer+b_done, b_write);
		bio_write(blk_num, blk_dir);
		blk_offset = 0;
		b_done += b_write;
		blk_ind++;
	}
    
	//part 2: indirect ptrs
	char* i_blk_dir = malloc(BLOCK_SIZE);
	int indirect_ind = 0, i_start = 0, i_blk_offset, blk_id = 0;
	if(!b_done){
		i_start = offset - (16*BLOCK_SIZE);
		i_blk_offset = i_start / BLOCK_SIZE;
		indirect_ind = i_blk_offset / (4*BLOCK_SIZE);
		i_start = i_blk_offset % (indirect_ind * 4 * BLOCK_SIZE);
	}
	while(b_done < size){
		if(inode->indirect_ptr[indirect_ind] == 0) inode->indirect_ptr[indirect_ind] = get_avail_blkno();
		blk_num = (inode->indirect_ptr[indirect_ind]) - 1 + super->d_start_blk;
		bio_read(blk_num, i_blk_dir);
		for(int i=i_start; i<BLOCK_SIZE; i+=4){
			blk_id = (*((int*)(i_blk_dir+i))) - 1;
			blk_id += super->d_start_blk;
			if(get_bitmap(dataBits, blk_id)){
				bio_read(blk_id, blk_dir);
				b_write = (size - b_done) > BLOCK_SIZE ? BLOCK_SIZE : size - b_done;
				memcpy(blk_dir+blk_offset, buffer+b_done, b_write);
				bio_write(blk_id, blk_dir);
				b_done += b_write;
				blk_offset = 0;
			}
		}
		bio_write(blk_num, i_blk_dir);
		i_start = 0;
		indirect_ind++;
	}

	//part 3: finishing up
	inode->size = (offset+size) > currentSize ? offset+size : currentSize;
	writei(inode->ino, inode);
	free(blk_dir);
	free(i_blk_dir);
	free(inode);
	printf("File written\n");
	return b_done;
}

static int rufs_unlink(const char *path) {
	printf("UNLINK (path = %s)\n", path);
	char* base_name = basename(strdup(path));
	inode = new_inode();
	int dir_ptr;
	if(get_node_by_path(path, rootNode->ino, inode)){
		printf("Cannot unlink(1)\n");
		return -ENOENT;
	}

	//part 1: direct ptrs
	for(int i=0; i<16; i++){
		dir_ptr = (inode->direct_ptr[i]) - 1;
		printf("Current dir_ptr being cleared = %d\n", inode->direct_ptr[i]);
		if(inode->direct_ptr[i] == 0) continue;
		unset_bitmap(dataBits, dir_ptr);
		inode->direct_ptr[i] = 0;
	}
	//part 2: indirect ptrs
	char* indirect_blk = malloc(BLOCK_SIZE);
	int blk_num, blk_id;
	for(int i=0; i<16; i++){
		if(inode->indirect_ptr[i] == 0) continue;
		blk_num = (inode->indirect_ptr[i]) - 1 + super->d_start_blk;
		bio_read(blk_num, indirect_blk);
		for(int j=0; j<(BLOCK_SIZE/4); j++){
			blk_id = (*((int*)(indirect_blk+(j*4)))) - 1;
			unset_bitmap(dataBits, blk_id);
		}
		inode->indirect_ptr[i] = 0;
	}
	free(indirect_blk);
	
	//part 3: removing for good
	int file_inode = inode->ino;
	dir_remove(*inode, base_name, strlen(base_name));
	free(inode);
	file_inode--;
	unset_bitmap(inodeBits, file_inode);

	printf("Unlinked\n");
    return 0;
}

static int rufs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations rufs_ope = {
	.init		= rufs_init,
	.destroy	= rufs_destroy,

	.getattr	= rufs_getattr,
	.readdir	= rufs_readdir,
	.opendir	= rufs_opendir,
	.releasedir	= rufs_releasedir,
	.mkdir		= rufs_mkdir,
	.rmdir		= rufs_rmdir,

	.create		= rufs_create,
	.open		= rufs_open,
	.read 		= rufs_read,
	.write		= rufs_write,
	.unlink		= rufs_unlink,

	.truncate   = rufs_truncate,
	.flush      = rufs_flush,
	.utimens    = rufs_utimens,
	.release	= rufs_release
};


int main(int argc, char *argv[]) {
	int fuse_stat;
	
	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");
	printf("Disk is going to %s\n", diskfile_path);
	fuse_stat = fuse_main(argc, argv, &rufs_ope, NULL);

	return fuse_stat;
}

