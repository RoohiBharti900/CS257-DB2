/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/
#if defined(_WIN32) || defined(_WIN64)
	#define _CRT_SECURE_NO_WARNINGS
#endif

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#define	EPOCH_YEAR	1970
#define	SECSPERHOUR	3600
#define	SECSPERDAY	86400
#define	TM_YEAR_BASE	1900

#if defined(_WIN32) || defined(_WIN64) 
#define snprintf _snprintf 
#define vsnprintf _vsnprintf 
#define strcasecmp _stricmp 
#define strncasecmp _strnicmp 
#endif


#define ALT_E			0x01
#define ALT_O			0x02
#define	LEGAL_ALT(x)		{ if (alt_format & ~(x)) return (0); }

#if defined(__CYGWIN__) || defined(__linux__)
    //for linux
    #include <unistd.h>
    #define stricmp strcasecmp
    #define strnicmp strncasecmp
    #define _stat stat
    #define _fstat fstat
    #define _fileno fileno
#else
    //for windows
    #define stricmp _stricmp
    #define unlink _unlink
    #define qsort_r qsort_s
	char *strptime(const char *buf, const char *fmt, struct tm *tm);
#endif

#if defined(__CYGWIN__)
    #define qsort_r //qsort_r
#endif

	static	int conv_num(const char **, int *, int, int);

	static const char *day[7] = {
		"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday",
		"Friday", "Saturday"
	};
	static const char *abday[7] = {
		"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
	};
	static const char *mon[12] = {
		"January", "February", "March", "April", "May", "June", "July",
		"August", "September", "October", "November", "December"
	};
	static const char *abmon[12] = {
		"Jan", "Feb", "Mar", "Apr", "May", "Jun",
		"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
	};
	static const char *am_pm[2] = {
		"AM", "PM"
	};



int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	if (! g_skip_init)
	{
		init_logarray();
		rc = initialize_tpd_list();
	}

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		strcpy(g_cmd, argv[1]);
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}
		else
		{
			if (! g_skip_init)
			{
				save_logarray();
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;
	char filename[255] = "";
	char tmpstr[255] = "";

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_SELECT) 
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if (cur->tok_value == K_DELETE)
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else if (cur->tok_value == K_UPDATE)
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TO)))
	{
		printf("BACKUP TO statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_RESTORE) &&
					((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("RESTORE FROM statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_ROLLFORWARD)
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: No DML or DDL allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						rc = sem_create_table(cur);
						if (rc == 0)
						{
							add_log_entry(g_cmd);
						}
						break;
			case DROP_TABLE:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: No DML or DDL allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						rc = sem_drop_table(cur);
						if (rc == 0)
						{
							add_log_entry(g_cmd);
						}
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: No DML or DDL allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						rc = sem_insert(cur);
						if (rc == 0)
						{
							add_log_entry(g_cmd);
						}
						break;
			case SELECT:
						rc = sem_select(cur);
						break;
			case DELETE:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: No DML or DDL allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						rc = sem_delete(cur);
						if (rc == 0)
						{
							add_log_entry(g_cmd);
						}
						break;
			case UPDATE:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: No DML or DDL allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						rc = sem_update(cur);
						if (rc == 0)
						{
							add_log_entry(g_cmd);
						}
						break;
			case BACKUP:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: Backup not allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						if (cur)
						{
							strcpy(filename, cur->tok_string);
						}

						rc = sem_backup(cur);
						if (rc == 0)
						{
							sprintf(tmpstr, "BACKUP %s", filename);
							add_log_entry(tmpstr);
						}
						break;
			case RESTORE:
						if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
						{
							printf("Error: Restore not allowed as Rollforward is Pending\n");
							rc = INVALID_STATEMENT;
							break;
						}

						rc = sem_restore(cur);
						break;
			case ROLLFORWARD:
						rc = sem_rollforward(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_rollforward(token_list *t_list)
{
	int rc = 0;
	token_list *cur = t_list;
	time_t rollforward_to = -1;
	int rfstart_entry = -1;
	int backup_entry = -1;
	
	if (!cur || (cur->tok_value != EOC && cur->tok_value != K_TO))
	{
		rc = INVALID_STATEMENT;
		printf("Error: expecting to or end of statement after rollforward\n");
		return rc;
	}

	rfstart_entry = find_rfstart_entry();
	if (rfstart_entry <= -1)
	{
		rc = INVALID_STATEMENT;
		printf("Error: could not find rf_start entry in the db.log file\n");
		return rc;
	}

	char *backup_filename = ((char *)(g_logarray[rfstart_entry].cmd)) + sizeof("RF_START ") - 1;
	printf("Info: backup file name is [%s]\n", backup_filename);
	backup_entry = find_backup_entry(backup_filename);
	if (backup_entry <= -1)
	{
		rc = INVALID_STATEMENT;
		printf("Error: could not find the corresponding backup entry\n");
		return rc;
	}

	if (cur->tok_value == EOC)
	{
		//prune rf_start entry only
		g_num_log_entries -=  1;
		if (save_logarray() != 0)
		{
			rc = INVALID_REPORT_FILE_NAME;
			printf("Error: Critical error, could not update log file, aborting restore\n");
			return rc;
		}
	}
	else
	{
		cur = cur->next;
		if (!cur || cur->tok_value != STRING_LITERAL)
		{
			rc = INVALID_STATEMENT;
			printf("Error:Invalid Timestamp format: expecting <timestamp> after to\n");
			return rc;
		}

		rollforward_to = convert_string_to_time(cur->tok_string);
		if (rollforward_to <= -1)
		{
			rc = INVALID_STATEMENT;
			printf("Error: syntax error in specified timestamp\n");
			return rc;
		}

		int rollforward_to_entry = find_processing_to_entry(backup_entry + 1, rollforward_to);
		if (rollforward_to_entry <= -1)
		{
			rc = INVALID_STATEMENT;
			printf("Error: could not determine the rollforward-to entry\n");
			return rc;
		}

		//prune rest of the entries
		g_num_log_entries = rollforward_to_entry + 1;
		if (save_logarray() != 0)
		{
			rc = INVALID_REPORT_FILE_NAME;
			printf("Error: Critical error, could not update log file, aborting restore\n");
			return rc;
		}
	}

	return process_redo(backup_entry + 1);
}

int sem_restore(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	char restore_file[255] = "";
	int without_rf_enabled = 0;

	cur = t_list;
	if (!cur || ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name)))
	{
		rc = INVALID_REPORT_FILE_NAME;
		printf("Error: Invalid image file name\n");
		return rc;
	}
	else
	{
		strcpy(restore_file, cur->tok_string);
		cur = cur->next;
		if (!cur || (cur->tok_value != EOC && cur->tok_value != K_WITHOUT))
		{
			rc = INVALID_STATEMENT;
			printf("Error: expecting without rf or end of statement\n");
			return rc;
		}
		else if (cur->tok_value == EOC)
		{
			without_rf_enabled = 0;
		}
		else if (cur->tok_value == K_WITHOUT)
		{
			cur = cur->next;
			if (!cur || cur->tok_value != K_RF)
			{
				rc = INVALID_STATEMENT;
				printf("Error: expecting rf after without\n");
				return rc;
			}
			without_rf_enabled = 1;
		}
	}

	int backup_entry = -1;
	if ((backup_entry = find_backup_entry(restore_file)) == -1)
	{
		rc = INVALID_REPORT_FILE_NAME;
		printf("Error: No backup entry with the specified filename exist\n");
		return rc;
	}

	if (backup_entry >= g_num_log_entries - 1)
	{
		printf("Warning: Nothing needs to be done, no action performed after the specified backup\n");
		return rc;
	}

	FILE* fp0 = fopen(restore_file, "rbc");
	if (!fp0)
	{
		rc = INVALID_REPORT_FILE_NAME;
		printf("Error: Could not open backup image file for reading\n");
		return rc;
	}

	if (without_rf_enabled)
	{
		g_num_log_entries = backup_entry + 1;
		if (save_logarray() != 0)
		{
			rc = INVALID_REPORT_FILE_NAME;
			printf("Error: Critical error, could not update log file, aborting restore\n");
			fclose(fp0);
			return rc;
		}
	}
	else
	{
		char tmpstr[255];
		sprintf(tmpstr, "RF_START %s", restore_file);
		add_log_entry(tmpstr);
		if (save_logarray() != 0)
		{
			rc = INVALID_REPORT_FILE_NAME;
			printf("Error: Critical error, could not update log file, aborting restore\n");
			fclose(fp0);
			return rc;
		}
	}

	int tpdlist_size = 0;
	fread(&tpdlist_size, sizeof(tpdlist_size), 1, fp0);
	char* ptr = (char *)malloc(tpdlist_size - sizeof(tpdlist_size));
	fread(ptr, tpdlist_size - sizeof(tpdlist_size), 1, fp0);

	if (! without_rf_enabled)
	{
		// *((int *)(ptr + sizeof(tpd_list::num_tables))) = ROLLFORWARD_PENDING;
		*((int *)(ptr + sizeof(int))) = ROLLFORWARD_PENDING;
	}
	
	FILE* fp1 = fopen("dbfile.bin", "wbc");
	fwrite(&tpdlist_size, sizeof(tpdlist_size), 1, fp1);
	fwrite(ptr, tpdlist_size - sizeof(tpdlist_size), 1, fp1);
	fclose(fp1);

	// tpd_entry* first = (tpd_entry*)(ptr + (sizeof(tpd_list)-sizeof(tpd_entry)-sizeof(tpd_list::list_size)));
	tpd_entry* first = (tpd_entry*)(ptr + (sizeof(tpd_list)-sizeof(tpd_entry)-sizeof(int)));
	tpd_entry* tpd = first;
	int num_tables = *((int *)ptr);
	for (int i = 0; i < num_tables; i++)
	{
		int filesize = 0;
		fread(&filesize, sizeof(filesize), 1, fp0);
		void* ptr1 = malloc(filesize);
		fread(ptr1, filesize, 1, fp0);

		fp1 = fopen(tpd->table_name, "wbc");
		fwrite(ptr1, filesize, 1, fp1);
		fclose(fp1);

		free(ptr1);
		tpd = (tpd_entry*)((char*)tpd + tpd->tpd_size);
	}

	free(ptr);
	fclose(fp0);
	return rc;
}

int sem_backup(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if (!cur || ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name)))
	{
		rc = INVALID_REPORT_FILE_NAME;
		printf("Error: Invalid image file name\n");
		cur->tok_value = INVALID;
		return rc;
	}
	else
	{
		struct _stat file_stat;
		FILE* fp0 = fopen(cur->tok_string, "rbc");
		if (fp0)
		{
			rc = INVALID_REPORT_FILE_NAME;
			printf("Error: Image file already exists\n");
			cur->tok_value = INVALID;
			fclose(fp0);
			return rc;
		}

		fp0 = fopen(cur->tok_string, "wbc");
		if (!fp0)
		{
			rc = INVALID_REPORT_FILE_NAME;
			printf("Error: Could not open image file for writing\n");
			return rc;
		}

		fwrite(g_tpd_list, g_tpd_list->list_size, 1, fp0);
        	tpd_entry *tpd = &(g_tpd_list->tpd_start);
        	int num_tables = g_tpd_list->num_tables;

		for (int i = 0; i < num_tables; i++)
		{
			FILE* fp1 = fopen(tpd->table_name, "rbc");
			_fstat(_fileno(fp1), &file_stat);
			int filesize = file_stat.st_size;

			void* ptr = malloc(filesize);
			fread(ptr, filesize, 1, fp1);
			fclose(fp1);

			fwrite(&filesize, sizeof(filesize), 1, fp0);
			fwrite(ptr, filesize, 1, fp0);
			free(ptr);

			tpd = (tpd_entry*)((char*)tpd + tpd->tpd_size);
		}
		fclose(fp0);
		return rc;
	}
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry* col_entry = NULL;
	int cur_id = 0;
	int i = 0;
	cur = t_list;

	filter_t filter;
	memset(&filter, 0, sizeof(filter));
	int filter_present = 0;

	cd_entry* col_entries[MAX_NUM_COL];
	column_t columns[MAX_NUM_COL];
	memset(&columns, 0, sizeof(columns));
	memset(&col_entries, 0, sizeof(col_entries));
	int n = 0;

	if (!cur || ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name)))
	{
		rc = INVALID_TABLE_NAME;
		printf("Error: Invalid table name\n");
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			printf("Error: Table does not exist\n");
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (!cur || stricmp(cur->tok_string, "set") != 0)
			{
				rc = INVALID_STATEMENT;
				printf("Error: Expecting 'set' after tablename\n");
				cur->tok_value = INVALID;
			}
			else
			{
				while (true)
				{
					cur = cur->next;
					if (!cur || ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name)))
					{
						rc = INVALID_COLUMN_NAME;
						printf("Error: Invalid column name\n");
						cur->tok_value = INVALID;
					}
					else
					{
						//we need to find the whether the column name that we mentioned in the statement exist in the table header or not
						col_entry = (cd_entry*)(tab_entry + 1);
						for (i = 0; i < tab_entry->num_columns; i++, col_entry++)
						{
							if (stricmp(cur->tok_string, col_entry->col_name) == 0)
							{
								break;
							}
						}
						if (i >= tab_entry->num_columns)
						{
							rc = INVALID_COLUMN_NAME;
							printf("Error: specified column does not exist in the table\n");
							cur->tok_value = INVALID;
						}
						else
						{
							cur = cur->next;
							if (!cur || (cur->tok_value != S_EQUAL))
							{
								rc = INVALID_STATEMENT;
								printf("Error: Expecting '=' after column name\n");
								cur->tok_value = INVALID;
							}
							else
							{
								cur = cur->next;
								if (!cur || (col_entry->col_type == T_INT && cur->tok_value != INT_LITERAL) ||
									(col_entry->col_type == T_CHAR && cur->tok_value != STRING_LITERAL))
								{
									rc = INVALID_STATEMENT;
									printf("Error: Column data type mismatch\n");
									cur->tok_value = INVALID;
								}
								else
								{
									col_entries[n] = col_entry;
									columns[n].type = col_entry->col_type;
									if (col_entry->col_type == T_INT)
									{
										columns[n].column_value.size = sizeof(int);
										columns[n].column_value.value.intVal = atoi(cur->tok_string);
									}
									else
									{
										if (strlen(cur->tok_string) > col_entry->col_len)
										{
											rc = INVALID_COLUMN_LENGTH;
											printf("Error: length of value exceeds the column size\n");
											cur->tok_value = INVALID;
										}
										else
										{
											columns[n].column_value.size = strlen(cur->tok_string);
											strcpy(columns[n].column_value.value.strVal, cur->tok_string);
										}
									}
								}
							}
						}
					}

					if (rc != 0)
					{
						printf("Error: invalid statement\n");
						break;
					}
					else
					{
						n += 1;
						cur = cur->next;
						if (cur && (cur->tok_value == EOC || cur->tok_value == K_WHERE))
						{
							break;
						}
						if (!cur || (cur->tok_value != S_COMMA && cur->tok_value != K_WHERE))
						{
							rc = INVALID_STATEMENT;
							printf("Error: expecting comma, where or end-of-command\n");
							cur->tok_value = INVALID;
							break;
						}
					}
				}
			}
		}
	}

		if (cur->tok_value == K_WHERE)
		{
			filter_present = 1;
			i = 0;
			cur = cur->next;
			
			while (i < 2)
			{
				col_entry = (cd_entry*)(tab_entry + 1);
				if (!cur || (filter.filter_col[i].col_id = get_column_id(tab_entry, cur)) == -1)
				{
					rc = INVALID_STATEMENT;
					printf("Error: invalid column specified in the where clause\n");
					return rc;
				}
				
				cur = cur->next;
				if (!cur || (cur->tok_value != S_EQUAL && cur->tok_value != S_LESS && cur->tok_value != S_GREATER))
				{
					rc = INVALID_STATEMENT;
					printf("Error: expecting = < > <= >= 1\n");
					return rc;
				}
				else if (cur->tok_value == S_EQUAL)
				{
					cur = cur->next;
					if (!cur || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL))
					{
						rc = INVALID_STATEMENT;
						printf("Error: expecting int or string literal after =\n");
						return rc;
					}
					else
					{
						filter.filter_col[i].col_op = S_EQUAL;
						if ((cur->tok_value == INT_LITERAL && col_entry[filter.filter_col[i].col_id].col_type != T_INT)
						|| (cur->tok_value == STRING_LITERAL && col_entry[filter.filter_col[i].col_id].col_type != T_CHAR))
						{
							rc = INVALID_STATEMENT;
							printf("Error: mismatch between column data type and filter value\n");
							return rc;
						}
						else
						{
							filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
							if (filter.filter_col[i].col_val.type == T_INT)
							{
								filter.filter_col[i].col_val.column_value.size = col_entry[filter.filter_col[i].col_id].col_len;
								filter.filter_col[i].col_val.column_value.value.intVal = atoi(cur->tok_string);
							}
							else
							{
								filter.filter_col[i].col_val.column_value.size = strlen(cur->tok_string);
								strcpy(filter.filter_col[i].col_val.column_value.value.strVal, cur->tok_string);
							}
						}
					}				
				}	
				else if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER)
				{
					filter.filter_col[i].col_op = cur->tok_value;
					cur = cur->next;
					if (cur && cur->tok_value == S_EQUAL)
					{
						filter.filter_col[i].col_op += cur->tok_value;
						cur = cur->next;
					}
					
					if (cur && cur->tok_value == INT_LITERAL)
					{
						filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
						filter.filter_col[i].col_val.column_value.size = col_entry[filter.filter_col[i].col_id].col_len;
						filter.filter_col[i].col_val.column_value.value.intVal = atoi(cur->tok_string);
					}
					else
					{
						filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
						filter.filter_col[i].col_val.column_value.size = strlen(cur->tok_string);
						strcpy(filter.filter_col[i].col_val.column_value.value.strVal, cur->tok_string);

						// FIX STRING COMPARISON
						// rc = INVALID_STATEMENT;
						// printf("Error: expecting integer literal after > < >= <=\n");
						// return rc;
					}
				}
				
				cur = cur->next;
				i += 1;
				if (!cur || (cur->tok_value != EOC && cur->tok_value != K_AND && cur->tok_value != K_OR))
				{
					rc = INVALID_STATEMENT;
					printf("Error: expecting and or or end of statement\n");
					return rc;
				}
				else if (cur->tok_value == K_AND || cur->tok_value == K_OR)
				{
					filter.mode = cur->tok_value;
					cur = cur->next;
				}
				else if (cur->tok_value == EOC)
				{
					break;
				}
			}
		}
		
		if (rc != 0)
		{
			printf("Error: something went wrong above\n");
			return rc;
		}

	printf("Info: n = %d\n", n);
	if (rc == 0)
	{
		FILE* fp = fopen(tab_entry->table_name, "rbc");
		FILE* fp1 = fopen("temp", "wbc");
		table_file_header tfh;
		fread(&tfh, sizeof(tfh), 1, fp);
		fwrite(&tfh, sizeof(tfh), 1, fp1);

		char* buffer = (char *)malloc(tfh.record_size);
		char **ptr = (char**)calloc(tab_entry->num_columns, sizeof(char*));
		col_entry = (cd_entry*)(tab_entry + 1);
		for (int j = 0; j < tab_entry->num_columns; j++)
		{
			ptr[j] = buffer;
			for (int k = 0; k < j; k++)
			{
				ptr[j] += col_entry[k].col_len + 1;
			}
			printf("Info: offset of %d is %d\n", j, ptr[j] - buffer);
		}

		int num_rows_matched = 0;
		for (i = 0; i < tfh.num_records; i++)
		{
			//int curloc = ftell(fp);
			fread(buffer, tfh.record_size, 1, fp);
			int offset = 0;

			column_t buffer_columns[MAX_NUM_COL];
			memset(buffer_columns, 0, sizeof(buffer_columns));

			col_entry = (cd_entry*)(tab_entry + 1);
			for (int k = 0; k < tab_entry->num_columns; k++, col_entry++)
			{
				//fread(&(buffer_columns[k].column_value.size), sizeof(buffer_columns[k].column_value.size), 1, fp);
				memcpy(&(buffer_columns[k].column_value.size), buffer + offset, sizeof(buffer_columns[k].column_value.size));

				//fread(&(buffer_columns[k].column_value.value), col_entry->col_len, 1, fp);
				memcpy(&(buffer_columns[k].column_value.value), buffer + offset + sizeof(buffer_columns[k].column_value.size), col_entry->col_len);

				buffer_columns[k].type = col_entry->col_type;
				offset += sizeof(buffer_columns[k].column_value.size) + col_entry->col_len;
			}

			if ((filter_present == 1 && row_matches_filter(tab_entry, buffer_columns, filter) == 1)
			|| (filter_present == 0))
			{
				num_rows_matched += 1;
				//printf("Info: processing row %d\n", i);
				for (int j = 0; j < n; j++)
				{
					//printf("Info: processing column %s\n", col_entries[j]->col_name);
					//printf("Info: size: [%d] [%d]\n", *(ptr[col_entries[j]->col_id]), columns[j].column_value.size);
					//printf("Info: (ptr[col_entries[j]->col_id]) = %x\n", (ptr[col_entries[j]->col_id]));
					*(ptr[col_entries[j]->col_id]) = columns[j].column_value.size;
					if (columns[j].type == T_INT)
					{
						//*((int *)(ptr[x] + 1)) = 10; //sample
						/*
						char* valptr = ptr[col_entries[j]->col_id] + 1;
						int* intptr = (int *)valptr;
						*intptr = columns[j].column_value.value.intVal;
						*/

						int tmp = columns[j].column_value.value.intVal;
						//printf("Info: (ptr[col_entries[j]->col_id] + 1) = %x\n", (ptr[col_entries[j]->col_id] + 1));
						//printf("Info: val: [%d] [%d]\n", *((int *)(ptr[col_entries[j]->col_id] + 1)), tmp);
						memcpy(ptr[col_entries[j]->col_id] + 1, &tmp, sizeof(tmp));
					
						//*((int *)(ptr[col_entries[j]->col_id] + 1)) = columns[j].column_value.value.intVal;
					}
					else
					{
						//printf("Info: val: [%s] [%s]\n", ptr[col_entries[j]->col_id] + 1, columns[j].column_value.value.strVal);
						memset(ptr[col_entries[j]->col_id] + 1, 0, col_entries[j]->col_len);
						strcpy(ptr[col_entries[j]->col_id] + 1, columns[j].column_value.value.strVal);
					}
				}
			}
			/*for (int j = 0; j < tfh.record_size; j++)
			{
				printf("%d:[%c] ", buffer[j], buffer[j]);
			}
			*/
			//printf("\n");
			//fseek(fp, curloc, SEEK_SET);
			fwrite(buffer, tfh.record_size, 1, fp1);
		}

		fclose(fp);
		fclose(fp1);
		unlink(tab_entry->table_name);
		rename("temp", tab_entry->table_name);

		printf("Info: %d rows updated\n", num_rows_matched);
	}

	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry* col_entry = NULL;
	int cur_id = 0;
	int i = 0;

	column_t	columns[MAX_NUM_COL];
	memset(&columns, 0, sizeof(columns));

	filter_t filter;
	memset(&filter, 0, sizeof(filter));
	int filter_present = 0;

	cur = t_list;
	if (!cur || stricmp(cur->tok_string, "from") != 0)
	{
		rc = INVALID_STATEMENT;
		printf("Error: Expecting from after keyword 'DELETE'\n");
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;
	}

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		rc = INVALID_TABLE_NAME;
		printf("Error: Invalid Table Name");
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			printf("Error: Table does not exist\n");
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (!cur || (cur->tok_value != K_WHERE && cur->tok_value != EOC))
			{
				rc = INVALID_STATEMENT;
				printf("Error: Expecting where or end of statement\n");
				return rc;
			}

			if (cur->tok_value == K_WHERE)
			{
				filter_present = 1;
				i = 0;
				cur = cur->next;
			
				while (i < 2)
				{
					col_entry = (cd_entry*)(tab_entry + 1);
					if (!cur || (filter.filter_col[i].col_id = get_column_id(tab_entry, cur)) == -1)
					{
						rc = INVALID_STATEMENT;
						printf("Error: invalid column specified in the where clause\n");
						return rc;
					}
				
					cur = cur->next;
					if (!cur || (cur->tok_value != S_EQUAL && cur->tok_value != S_LESS && cur->tok_value != S_GREATER))
					{
						rc = INVALID_STATEMENT;
						printf("Error: expecting = < > <= >= 1\n");
						return rc;
					}
					else if (cur->tok_value == S_EQUAL)
					{
						cur = cur->next;
						if (!cur || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL))
						{
							rc = INVALID_STATEMENT;
							printf("Error: expecting int or string literal after =\n");
							return rc;
						}
						else
						{
							filter.filter_col[i].col_op = S_EQUAL;
							if ((cur->tok_value == INT_LITERAL && col_entry[filter.filter_col[i].col_id].col_type != T_INT)
							|| (cur->tok_value == STRING_LITERAL && col_entry[filter.filter_col[i].col_id].col_type != T_CHAR))
							{
								rc = INVALID_STATEMENT;
								printf("Error: mismatch between column data type and filter value\n");
								return rc;
							}
							else
							{
								filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
								if (filter.filter_col[i].col_val.type == T_INT)
								{
									filter.filter_col[i].col_val.column_value.size = col_entry[filter.filter_col[i].col_id].col_len;
									filter.filter_col[i].col_val.column_value.value.intVal = atoi(cur->tok_string);
								}
								else
								{
									filter.filter_col[i].col_val.column_value.size = strlen(cur->tok_string);
									strcpy(filter.filter_col[i].col_val.column_value.value.strVal, cur->tok_string);
								}
							}
						}				
					}	
					else if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER)
					{
						filter.filter_col[i].col_op = cur->tok_value;
						cur = cur->next;
						if (cur && cur->tok_value == S_EQUAL)
						{
							filter.filter_col[i].col_op += cur->tok_value;
							cur = cur->next;
						}
					
						if (cur && cur->tok_value == INT_LITERAL)
						{
							filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
							filter.filter_col[i].col_val.column_value.size = col_entry[filter.filter_col[i].col_id].col_len;
							filter.filter_col[i].col_val.column_value.value.intVal = atoi(cur->tok_string);
						}
						else
						{
							filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
							filter.filter_col[i].col_val.column_value.size = strlen(cur->tok_string);
							strcpy(filter.filter_col[i].col_val.column_value.value.strVal, cur->tok_string);

							// FIX STRING COMPARISON
							// rc = INVALID_STATEMENT;
							// printf("Error: expecting integer literal after > < >= <=\n");
							// return rc;
						}
					}
				
					cur = cur->next;
					i += 1;
					if (!cur || (cur->tok_value != EOC && cur->tok_value != K_AND && cur->tok_value != K_OR))
					{
						rc = INVALID_STATEMENT;
						printf("Error: expecting and or or end of statement\n");
						return rc;
					}
					else if (cur->tok_value == K_AND || cur->tok_value == K_OR)
					{
						filter.mode = cur->tok_value;
						cur = cur->next;
					}
					else if (cur->tok_value == EOC)
					{
						break;
					}
				}
			}
		
			if (rc != 0)
			{
				printf("Error: something went wrong above\n");
				return rc;
			}

			if (rc == 0)
			{
				FILE* fp = fopen(tab_entry->table_name, "rbc");
				FILE* fp1 = fopen("temp", "wbc");
				table_file_header tfh;
				fread(&tfh, sizeof(tfh), 1, fp);
				fwrite(&tfh, sizeof(tfh), 1, fp1);
				int num_rows_matched = 0;

				for (i = 0; i < tfh.num_records; i++)
				{
					column_t columns[MAX_NUM_COL];
					memset(columns, 0, sizeof(columns));

					col_entry = (cd_entry*)(tab_entry + 1);
					for (int k = 0; k < tab_entry->num_columns; k++, col_entry++)
					{
						fread(&(columns[k].column_value.size), sizeof(columns[k].column_value.size), 1, fp);
						fread(&(columns[k].column_value.value), col_entry->col_len, 1, fp);
						columns[k].type = col_entry->col_type;
					}

					if ((filter_present == 1 && row_matches_filter(tab_entry, columns, filter) == 1)
					|| (filter_present == 0))
					{
						num_rows_matched += 1;
					}
					else
					{
						col_entry = (cd_entry*)(tab_entry + 1);
						for (int k = 0; k < tab_entry->num_columns; k++, col_entry++)
						{
							fwrite(&(columns[k].column_value.size), sizeof(columns[k].column_value.size), 1, fp1);
							fwrite(&(columns[k].column_value.value), col_entry->col_len, 1, fp1);
							columns[k].type = col_entry->col_type;
						}
					}
				}

				fclose(fp);
				fclose(fp1);
				unlink(tab_entry->table_name);
				rename("temp", tab_entry->table_name);

				fp = fopen(tab_entry->table_name, "r+bc");
				fseek(fp, 0, SEEK_SET);
				tfh.file_size -= tfh.record_size * num_rows_matched;
				tfh.num_records -= num_rows_matched;
				fwrite(&tfh, sizeof(tfh), 1, fp);
				fclose(fp);

				printf("Info: %d rows deleted\n", num_rows_matched);
			}
		}
	}

	if (rc != 0)
	{
		rc = INVALID_STATEMENT;
		printf("Error: Invalid statement\n");
		cur->tok_value = INVALID;
	}

	return rc;
}

int row_matches_filter(tpd_entry* tab_entry, column_t* columns, filter_t& filter)
{
	int i = 0;
	int result = 0;

	while (i < 2)
	{
		int &col_to_match = filter.filter_col[i].col_id;
		char* lstrval = columns[col_to_match].column_value.value.strVal;
		char* rstrval = filter.filter_col[i].col_val.column_value.value.strVal;
		int& lintval = columns[col_to_match].column_value.value.intVal;
		int& rintval = filter.filter_col[i].col_val.column_value.value.intVal;

		if (columns[col_to_match].type == T_INT)
		{
			switch (filter.filter_col[i].col_op)
			{
			case S_EQUAL:
				if (lintval == rintval)
				{
					result += 1;
				}
				break;

			case S_LESS:
				if (lintval < rintval)
				{
					result += 1;
				}
				break;

			case S_GREATER:
				if (lintval > rintval)
				{
					result += 1;
				}
				break;

			case S_EQUAL + S_LESS:
				if (lintval <= rintval)
				{
					result += 1;
				}
				break;

			case S_EQUAL + S_GREATER:
				if (lintval >= rintval)
				{
					result += 1;
				}
				break;

			default:
				printf("Error: not possible, invalid operator\n");
				return 0;
			}
		}
		else if (columns[col_to_match].type == T_CHAR)
		{
			switch (filter.filter_col[i].col_op)
			{
			case S_EQUAL:
				if (strcmp(lstrval, rstrval) == 0)
				{
					result += 1;
				}
				break;

			case S_LESS:
				if (strcmp(lstrval, rstrval) < 0)
				{
					result += 1;
				}
				break;

			case S_GREATER:
				if (strcmp(lstrval, rstrval) > 0)
				{
					result += 1;
				}
				break;

			case S_EQUAL + S_LESS:
				if (strcmp(lstrval, rstrval) <= 0)
				{
					result += 1;
				}
				break;

			case S_EQUAL + S_GREATER:
				if (strcmp(lstrval, rstrval) >= 0)
				{
					result += 1;
				}
				break;

			default:
				printf("Error: invalid operator for string comparison\n");
				printf("Error: not possible, invalid operator\n");
				return 0;
			}
		}

		i += 1;
		if ((result == 1 && filter.mode == K_OR)
		|| (result == 0 && filter.mode == K_AND)
		|| (filter.mode == 0))
		{
			break;
		}
	}

	if ((filter.mode == K_AND && result >= 2)
	|| (filter.mode == K_OR && result >= 1)
	|| (filter.mode == 0 && result >= 1))
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

int get_column_id(tpd_entry* tab_entry, token_list* cur)
{
	cd_entry* col_entry = (cd_entry*)(tab_entry + 1);
	for (int i = 0; i < tab_entry->num_columns; i++)
	{
		if (stricmp(col_entry[i].col_name, cur->tok_string) == 0)
		{
			return col_entry[i].col_id;
		}
	}
	return -1;
}

#if defined(__CYGWIN__) || defined(__linux__)
    //for linux
    int rowcompare(const void* v1, const void* v2, void* v3)
#else
    //for windows
    int rowcompare(void* v3, const void* v1, const void* v2)
#endif
{
	column_t* row1 = (column_t*)v1;
	column_t* row2 = (column_t*)v2;
	int order_by_col = ((orderby_t*)(v3))->order_by_col;
	int order_by_used = ((orderby_t*)(v3))->order_by_used;

	if (row1[order_by_col].type == T_INT)
	{
		if (order_by_used == 2)
		{
			return row2[order_by_col].column_value.value.intVal - row1[order_by_col].column_value.value.intVal;
		}
		else
		{
			return row1[order_by_col].column_value.value.intVal - row2[order_by_col].column_value.value.intVal;
		}
	}
	else 
	{
		if (order_by_used == 1)
		{
			return strcmp(row1[order_by_col].column_value.value.strVal, row2[order_by_col].column_value.value.strVal);
		}
		else
		{
			return strcmp(row2[order_by_col].column_value.value.strVal, row1[order_by_col].column_value.value.strVal);
		}
	}
}

int sem_select(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry* col_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	int i = 0, j = 0, k = 0;
	int cols[MAX_NUM_COL];
	memset(cols, 0, sizeof(cols));

	int num_selected_cols = 0;
	int is_select_star = 0;
	char column_names[MAX_NUM_COL][MAX_IDENT_LEN+1];
	memset(column_names, 0, sizeof(column_names));

	int aggr_function_used = 0;
	int aggr_function[MAX_NUM_COL];
	memset(aggr_function, 0, sizeof(aggr_function));

	int aggr_result[MAX_NUM_COL];
	memset(aggr_result, 0, sizeof(aggr_result));

	filter_t filter;
	memset(&filter, 0, sizeof(filter));
	int filter_present = 0;

	orderby_t orderby;
	memset(&orderby, 0, sizeof(orderby));

	cur = t_list; 
	
	if (!cur || ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name) &&
		(cur->tok_value != S_STAR) &&
		(cur->tok_value != F_SUM) &&
		(cur->tok_value != F_AVG) &&
		(cur->tok_value != F_COUNT)) || (stricmp(cur->tok_string, "from") == 0))
	{
		rc = INVALID_STATEMENT;
		printf("Error: Expecting sum, avg, count, column name or '*' after SELECT\n");
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->tok_value == S_STAR)
		{
			is_select_star = 1;
			cur = cur->next;
			if (!cur || stricmp(cur->tok_string, "from") != 0)
			{
				rc = INVALID_STATEMENT;
				printf("Error: Expecting from\n");
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
			}
		}
		else
		{
			while(true)
			{
				if (cur->tok_value == F_SUM || cur->tok_value == F_AVG || cur->tok_value == F_COUNT)
				{
					aggr_function_used = 1;
					aggr_function[i] = cur->tok_value;
					cur = cur->next;
					if (!cur || cur->tok_value != S_LEFT_PAREN)
					{
						rc = INVALID_STATEMENT;
						printf("Error: Expecting ( after the aggregate function\n");
						return rc;
					}
					else
					{
						cur = cur->next;
						if ((aggr_function[i] == F_SUM || aggr_function[i] == F_AVG) &&
							cur->tok_value == S_STAR)
						{
							rc = INVALID_STATEMENT;
							printf("Error: sum or avg functions cannot use * as argument\n");
							return rc;
						}

						strcpy(column_names[i++], cur->tok_string);
						cur = cur->next;
						if (!cur || cur->tok_value != S_RIGHT_PAREN)
						{
							rc = INVALID_STATEMENT;
							printf("Error: Expecting ) after the aggregate function\n");
						}
					}
				}
				else
				{
					strcpy(column_names[i++], cur->tok_string);
				}

				cur = cur->next;
				if (!cur || (cur->tok_value != S_COMMA && stricmp(cur->tok_string, "from") != 0))
				{
					rc = INVALID_STATEMENT;
					printf("Error: Expecting comma or from\n");
					break;
				}
				else if (cur->tok_value == S_COMMA)
				{
					cur = cur->next;
					if (!cur || ((cur->tok_class != keyword) &&
		  				(cur->tok_class != identifier) &&
						(cur->tok_class != type_name) &&
						(cur->tok_value != F_SUM) &&
						(cur->tok_value != F_AVG) &&
						(cur->tok_value != F_COUNT)) || (stricmp(cur->tok_string, "from") == 0))
					{
						rc = INVALID_STATEMENT;
						printf("Error: Expecting sum, avg, count or column name after ','\n");
						cur->tok_value = INVALID;
						break;
					}
				}
				else
				{
					cur = cur->next;
					break;
				}
			}
		}

		if (rc != 0)
		{
			printf("Error: Invalid statement\n");
			return rc;
		}

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			rc = INVALID_TABLE_NAME;
			printf("Error: Expecting table name\n");
			cur->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				printf("Error: Table does not exist\n");
                
			}
			else
			{
				if (is_select_star != 1)
				{
					for (j = 0; j < i; j++)
					{
						if (stricmp(column_names[j], "*") == 0)
						{
							cols[j] = 0;
							continue;
						}

						col_entry = (cd_entry*)(tab_entry + 1);
						for (k = 0; k < tab_entry->num_columns; k++, col_entry++)
						{
							if (stricmp(column_names[j], col_entry->col_name) == 0)
							{
								cols[j] = k;
								if (aggr_function_used == 1)
								{
									if ((aggr_function[j] == F_SUM || aggr_function[j] == F_AVG) &&
										col_entry->col_type == T_CHAR)
									{
										rc = INVALID_STATEMENT;
										printf("Error: sum or avg function cannot be used with [%s] column\n", col_entry->col_name);
										break;
									}
								}
								break;
							}
						}
						if (k >= tab_entry->num_columns)
						{
							rc = INVALID_COLUMN_NAME;
							printf("Error: column not defined in the table\n");
							break;
						}
					}
				}
			}
		}

		if (rc != 0)
		{  
			printf("Error In Select statement\n");
			return rc;
		}
		num_selected_cols = i;
		
		cur = cur->next;
		if (!cur || (cur->tok_value != EOC && cur->tok_value != K_WHERE && cur->tok_value != K_ORDER))
		{
			rc = INVALID_STATEMENT;
			printf("Error: Invalid statement, expecting where or order-by or end of select\n");
			return rc;
		}
		
		if (cur->tok_value == K_WHERE)
		{
			filter_present = 1;
			i = 0;
			cur = cur->next;
			
			while (i < 2)
			{
				col_entry = (cd_entry*)(tab_entry + 1);
				if (!cur || (filter.filter_col[i].col_id = get_column_id(tab_entry, cur)) == -1)
				{
					rc = INVALID_STATEMENT;
					printf("Error: invalid column specified in the where clause\n");
					return rc;
				}
				
				cur = cur->next;
				if (!cur || (cur->tok_value != S_EQUAL && cur->tok_value != S_LESS && cur->tok_value != S_GREATER))
				{
					rc = INVALID_STATEMENT;
					printf("Error: expecting = < > <= >= 1\n");
					return rc;
				}
				else if (cur->tok_value == S_EQUAL)
				{
					cur = cur->next;
					if (!cur || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL))
					{
						rc = INVALID_STATEMENT;
						printf("Error:Invalid relational operator: expecting int or string literal after =\n");
						return rc;
					}
					else
					{
						filter.filter_col[i].col_op = S_EQUAL;
						if ((cur->tok_value == INT_LITERAL && col_entry[filter.filter_col[i].col_id].col_type != T_INT)
						|| (cur->tok_value == STRING_LITERAL && col_entry[filter.filter_col[i].col_id].col_type != T_CHAR))
						{
							rc = INVALID_STATEMENT;
							printf("Error: mismatch between column data type and filter value\n");
							return rc;
						}
						else
						{
							filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
							if (filter.filter_col[i].col_val.type == T_INT)
							{
								filter.filter_col[i].col_val.column_value.size = col_entry[filter.filter_col[i].col_id].col_len;
								filter.filter_col[i].col_val.column_value.value.intVal = atoi(cur->tok_string);
							}
							else
							{
								filter.filter_col[i].col_val.column_value.size = strlen(cur->tok_string);
								strcpy(filter.filter_col[i].col_val.column_value.value.strVal, cur->tok_string);
							}
						}
					}				
				}	
				else if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER)
				{
					filter.filter_col[i].col_op = cur->tok_value;
					cur = cur->next;

   
					if (cur && cur->tok_value == S_EQUAL)
					{
						filter.filter_col[i].col_op += cur->tok_value;
						cur = cur->next;
					}
					
					if (cur && cur->tok_value == INT_LITERAL)
					{
						filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
						filter.filter_col[i].col_val.column_value.size = col_entry[filter.filter_col[i].col_id].col_len;
						filter.filter_col[i].col_val.column_value.value.intVal = atoi(cur->tok_string);
					}
					else
					{
						filter.filter_col[i].col_val.type = col_entry[filter.filter_col[i].col_id].col_type;
						filter.filter_col[i].col_val.column_value.size = strlen(cur->tok_string);
						strcpy(filter.filter_col[i].col_val.column_value.value.strVal, cur->tok_string);
                 	
                   
					 // FIX STRING COMPARISON
					//	 rc = INVALID_STATEMENT;
					//	 printf("Error:Invalid relational operator:expecting integer literal after > < >= <=\n");
					//	 return rc;
                    }
                     
				}
				
				cur = cur->next;
				i += 1;
				if (!cur || (cur->tok_value != EOC && cur->tok_value != K_AND && cur->tok_value != K_OR && cur->tok_value != K_ORDER))
				{
					rc = INVALID_STATEMENT;
					printf("Error: expecting and or order or end of statement\n");
					return rc;
				}
				else if (cur->tok_value == K_AND || cur->tok_value == K_OR)
				{
					filter.mode = cur->tok_value;
					cur = cur->next;
				}
				else if (cur->tok_value == EOC || cur->tok_value == K_ORDER)
				{
					break;
				}
			}
		}
		
		if (rc != 0)
		{
			printf("Error: something went wrong above\n");
			return rc;
		}

		if (cur->tok_value == K_ORDER)
		{
			cur = cur->next;
			if (!cur || cur->tok_value != K_BY)
			{
				rc = INVALID_STATEMENT;
				printf("Error: Expecting by after order\n");
				return rc;
			}

			cur = cur->next;
			if (!cur || (orderby.order_by_col = get_column_id(tab_entry, cur)) == -1)
			{
				rc = INVALID_STATEMENT;
				printf("Error: Invalid column specified in the order by clause\n");
				return rc;
			}

			orderby.order_by_used = 1;
			cur = cur->next;
			if (!cur || (cur->tok_value != EOC && cur->tok_value != K_DESC))
			{
				rc = INVALID_STATEMENT;
				printf("Error: Expecting desc or end of select\n");
				return rc;
			}
			else if (cur->tok_value == K_DESC)
			{
				orderby.order_by_used = 2;
			}
		}

		FILE* fp = fopen(tab_entry->table_name, "rbc");
		table_file_header tfh;
		fread(&tfh, sizeof(tfh), 1, fp);

		if (is_select_star == 1)
		{
			num_selected_cols = tab_entry->num_columns;
			col_entry = (cd_entry*)(tab_entry + 1);
			for (j = 0; j < num_selected_cols; j++, col_entry++)
			{
				cols[j] = j;
			}
		}

		col_entry = (cd_entry*)(tab_entry + 1);
		for (j = 0; j < num_selected_cols; j++)
		{
			switch(aggr_function[j])
			{
				case F_SUM:
					printf("sum(%s)", col_entry[cols[j]].col_name);
					break;

				case F_AVG:
					printf("avg(%s)", col_entry[cols[j]].col_name);
					break;

				case F_COUNT:
					printf("count(%s)", col_entry[cols[j]].col_name);
					break;

				default:
					printf("%10s|", col_entry[cols[j]].col_name);
					break;
			}

			if (j < num_selected_cols - 1)
			{
				printf("");
			}
		}
		printf("\n");
		printf("------------------------------------------------------------------\n");
			
		column_t table_data[100][MAX_NUM_COL];
		memset(table_data, 0, sizeof(table_data));
		int num_matched_rows = 0;
		
		for (j = 0; j < tfh.num_records; j++)
		{
			column_t columns[MAX_NUM_COL];
			memset(columns, 0, sizeof(columns));

			col_entry = (cd_entry*)(tab_entry + 1);
			for (k = 0; k < tab_entry->num_columns; k++, col_entry++)
			{
				fread(&(columns[k].column_value.size), sizeof(columns[k].column_value.size), 1, fp);
				fread(&(columns[k].column_value.value), col_entry->col_len, 1, fp);
				columns[k].type = col_entry->col_type;
			}
			
			if ((filter_present == 1 && row_matches_filter(tab_entry, columns, filter) == 1)
			|| (filter_present == 0))
			{
				memcpy(table_data[num_matched_rows++], columns, sizeof(columns));
				col_entry = (cd_entry*)(tab_entry + 1);
				for (k = 0; k < num_selected_cols; k++)
				{
					if (aggr_function[k] == F_COUNT)
					{
						aggr_result[k] += 1;
					}

					if (columns[cols[k]].type == T_INT)
					{
						if (aggr_function[k] == F_SUM || aggr_function[k] == F_AVG)
						{
							aggr_result[k] += columns[cols[k]].column_value.value.intVal;
						}
						
						//printf("%d", columns[cols[k]].column_value.value.intVal);
					}
					else
					{
						//printf("%s", columns[cols[k]].column_value.value.strVal);
						
					}
					if (k < num_selected_cols - 1)
					{
						//printf(",");
					}
				}
				//printf("\n");
			}
		}
		//printf("------------------------------------------------------------------\n");

		if (orderby.order_by_used > 0)
		{
			qsort_r(table_data, num_matched_rows, sizeof(table_data[0]), rowcompare, &orderby);
		}

		if (aggr_function_used == 1)
		{
			for (i = 0; i < num_selected_cols; i++)
			{
				if (aggr_function[i] == F_AVG)
				{
					aggr_result[i] = aggr_result[i] / num_matched_rows;
				}
				printf("%d", aggr_result[i]);
				if (i < num_selected_cols - 1)
				{
					printf(",");
				}
			}
			printf("\n");
			printf("------------------------------------------------------------------\n");
		}
		else
		{
			for (j = 0; j < num_matched_rows; j++)
			{
				column_t* columns = table_data[j];
				col_entry = (cd_entry*)(tab_entry + 1);
			
				for (k = 0; k < num_selected_cols; k++)
				{
					if (columns[cols[k]].type == T_INT)
					{
						printf("%10d|", columns[cols[k]].column_value.value.intVal);
					}
					else
					{
						printf("%10s|", columns[cols[k]].column_value.value.strVal);
					}
					if (k < num_selected_cols - 1)
					{
						printf("");
					}
				}
				printf("\n");
			}
			printf("------------------------------------------------------------------\n");
		}
		fclose(fp);
	}
  return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry* col_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	int i = 0;

	column_t	columns[MAX_NUM_COL];
	memset(&columns, 0, sizeof(columns));


	cur = t_list;
	if (!cur || ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name)))
	{
		rc = INVALID_TABLE_NAME;
		printf("Error:Invalid Table Name");
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			printf("Error: Table does not exist\n");
			cur->tok_value = INVALID;
		}
		else
		{
			col_entry = (cd_entry*)(tab_entry + 1);
			cur = cur->next;
			if (!cur || cur->tok_value != K_VALUES)
			{
				rc = INVALID_STATEMENT;
				printf("Error: Missing keyword 'values' in insert statement\n");
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if (!cur || cur->tok_value != S_LEFT_PAREN)
				{
					rc = INVALID_STATEMENT;
					printf("Error: Missing left parenthesis\n");
					cur->tok_value = INVALID;
				}
				else
				{
					for (i = 0; i < tab_entry->num_columns; i++, col_entry++)
					{
						cur = cur->next;
						printf("Info: Processing %s, %d, %d\n", col_entry->col_name, col_entry->col_type, cur->tok_value);
						if (!cur || cur->tok_value == EOC)
						{
							rc = TABLE_NOT_EXIST;
							printf("Error: Expecting column values\n");
							cur->tok_value = INVALID;
							break;
						}
						else if (!cur || (col_entry->col_type == T_INT && cur->tok_value != INT_LITERAL) ||
								(col_entry->col_type == T_CHAR && cur->tok_value != STRING_LITERAL))
						{
							rc = INVALID_TYPE_NAME;
							printf("Error: Column data type mismatch\n");
							cur->tok_value = INVALID;
							break;
						}
						else
						{
							columns[i].type = col_entry->col_type;
							columns[i].column_value.size = col_entry->col_len;
							if (col_entry->col_type == T_INT)
							{
								columns[i].column_value.value.intVal = atoi(cur->tok_string);
							}
							else
							{
								if (strlen(cur->tok_string) > col_entry->col_len)
								{
									rc = INVALID_COLUMN_LENGTH;
									printf("Error: Length of value exceeds the column size\n");
									cur->tok_value = INVALID_COLUMN_LENGTH;
									break;
								}
								else
								{
									strcpy(columns[i].column_value.value.strVal, cur->tok_string);
								}
							}
						}
						cur = cur->next;
						if (!cur || (cur->tok_value != S_RIGHT_PAREN && cur->tok_value != S_COMMA))
						{
							rc = INVALID_STATEMENT;
							printf("Error: Missing comma or right parenthesis\n");
							cur->tok_value = INVALID;
							break;
						}
					}
					
					if (rc != 0)
					{
						rc = TABLE_NOT_EXIST;
						printf("Error: Invalid statement\n");
						cur->tok_value = INVALID;
					}
					else
					{
						char buffer[256];
						char *ptr = buffer;
						FILE* fp = fopen(tab_entry->table_name, "r+bc");

						table_file_header tfh;
						fseek(fp, 0, SEEK_SET);
						fread(&tfh, sizeof(tfh), 1, fp);
						tfh.file_size += tfh.record_size;
						tfh.num_records += 1;
						fseek(fp, 0, SEEK_SET);
						fwrite(&tfh, sizeof(tfh), 1, fp);
						fseek(fp, 0, SEEK_END);

						for (i=0; i<tab_entry->num_columns; i++)
						{
							printf("%02d, %02d, %02d, ", i, columns[i].type, columns[i].column_value.size);
							if ( columns[i].type == T_INT)
							{
								printf("%d\n", columns[i].column_value.value.intVal);
								*ptr = sizeof(int);
								memcpy(ptr + 1, &(columns[i].column_value.value.intVal), sizeof(int));
								fwrite(buffer, sizeof(int) + 1, 1, fp);
							}
							else
							{
								printf("%s\n", columns[i].column_value.value.strVal);
								*ptr = strlen(columns[i].column_value.value.strVal);
								memcpy(ptr + 1, columns[i].column_value.value.strVal, columns[i].column_value.size);
								fwrite(buffer, 1 + columns[i].column_value.size, 1, fp);
							}
						}

						fclose(fp);
					}
				}
			}
		}
	}
  return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		printf("Error:Invalid tablename");
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			printf("Error:Table already exist with same name");
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				printf("Error:Expecting a '(' after table name");
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						printf("Error:The column name is not valid");
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								printf("Error:Duplicate coulumn name for the table");
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								printf("Error:Data type mismatch");
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_STATEMENT;
										printf("Error:The Statement is not valid");
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_STATEMENT;
											printf("Error:Missing NULL after NOT");
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_STATEMENT;
												printf("Error:Missing ',' or ')'");
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_STATEMENT;
										printf("Error:Missing '('");
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_TYPE_NAME;
											printf("Error:The current token value is not valid");
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_STATEMENT;
												printf("Error:Expecting a ')'");
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_STATEMENT;
															printf("Error: missing ')' or ','");
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %ld\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			//fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int save_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		printf("Error: error in opening dbfile.bin for writing\n");
		rc = FILE_OPEN_ERROR;
		return rc;
	}

	fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);
	fclose(fhandle);
	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;
	FILE *thandle = NULL;
	cd_entry* cd = NULL;
	table_file_header tabHeader;
	int tabOffset = 0;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		tabOffset = ftell(fhandle);
		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	if ((thandle = fopen(tpd->table_name, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		tabHeader.file_size = sizeof(tabHeader);
		tabHeader.record_size = 0;

		cd = (cd_entry*)(tpd + 1);
		for (int i = 0; i < tpd->num_columns; i++)
		{
			tabHeader.record_size += cd[i].col_len + 1;
		}
		
		tabHeader.num_records = 0;
		tabHeader.record_offset = sizeof(tabHeader);
		tabHeader.file_header_flag = 0;
		tabHeader.tpd_ptr = NULL;
		fwrite(&tabHeader, sizeof(tabHeader), 1, thandle);
		fflush(thandle);
		fclose(thandle);
	}

	free(g_tpd_list);
	g_tpd_list = NULL;
	return initialize_tpd_list();
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
					unlink(tabname);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int init_logarray()
{
	memset(g_logarray, 0, sizeof(g_logarray));
	g_num_log_entries = 0;

	FILE* fp0 = fopen("db.log", "r");
	if (!fp0)
	{
		return 0;
	}

	char buffer[400];
	while(1)
	{
		memset(buffer, 0, sizeof(buffer));
		if(fgets(buffer, sizeof(buffer), fp0) == NULL)
		{
			break;
		}

		//remove the newline character
		*(strstr(buffer, "\n")) = 0;

		buffer[14] = 0;
		struct tm time_info;
		memset(&time_info, 0, sizeof(time_info));

		strptime(buffer, "%Y%m%d%H%M%S", &time_info);
		time_info.tm_isdst = -1;
		time_t unix_time = mktime(&time_info);
		char* cmd = &buffer[15];
		
		g_logarray[g_num_log_entries].time = unix_time;
		strcpy(g_logarray[g_num_log_entries].cmd, cmd);
		g_num_log_entries += 1;
	}

	fclose(fp0);
	return 0;
}

time_t convert_string_to_time(char* str_time)
{
	struct tm time_info;
	memset(&time_info, 0, sizeof(time_info));
	time_info.tm_isdst = -1;

	if (strptime(str_time, "%Y%m%d%H%M%S", &time_info) == NULL)
	{
		printf("Error: syntax error in specified timestamp\n");
		return -1;
	}

	return mktime(&time_info);
}

int add_log_entry(char* cmd)
{
	if (! g_skip_init)
	{
		g_logarray[g_num_log_entries].time = time(NULL);
		strcpy(g_logarray[g_num_log_entries].cmd, cmd);
		g_num_log_entries += 1;
	}

	return 0;
}

int save_logarray()
{
	FILE* fp0 = fopen("db.log", "w");
	if (!fp0)
	{
		printf("Error: Could not create db.log file\n");
		return 1;
	}

	char str_time[20];
	for (int i = 0; i < g_num_log_entries; i++ )
	{
		strftime(str_time, sizeof(str_time), "%Y%m%d%H%M%S", localtime(&(g_logarray[i].time)));
		fprintf(fp0, "%s %s\n", str_time, g_logarray[i].cmd);
	}

	fclose(fp0);
	return 0;
}

int find_backup_entry(char* filename)
{
	for (int i = 0; i < g_num_log_entries; i++)
	{
		char* cmd = g_logarray[i].cmd;
		char tmpstr[255];
		sprintf(tmpstr, "BACKUP %s", filename);
		if (strstr(cmd, tmpstr) == cmd)
		{
			return i;
		}
	}

	return -1;
}

int find_rfstart_entry()
{
	for (int i = g_num_log_entries - 1; i >= 0; i--)
	{
		char* cmd = g_logarray[i].cmd;
		if (strstr(cmd, "RF_START ") == cmd)
		{
			return i;
		}
	}

	return -1;
}

int find_processing_to_entry(int processing_start, time_t processing_to)
{
	for (int i = g_num_log_entries - 2; i >= processing_start; i--)
	{
		if (g_logarray[i].time > processing_to)
		{
			continue;
		}
		else
		{
			return i;
		}
	}

	return -1;
}

int process_redo(int start)
{
	g_tpd_list->db_flags = 0;
	if (save_tpd_list() <= -1)
	{
		printf("Error: error in saving tpd_list, aborting\n");
		return -1;
	}

	char* cmd = NULL;
	g_skip_init = 1;
	for (int i = start; i < g_num_log_entries; i++)
	{
		cmd = g_logarray[i].cmd;
		if (strstr(cmd, "BACKUP ") == cmd)
		{
			continue;
		}
		
		printf("Info: Executing [%s]\n", cmd);
		char *tmpstr[] = {"", cmd};
		if (main(2, tmpstr) != 0)
		{
			printf("Error: one of the redo command failed, failed command is [%s]\n", cmd);
			printf("Error: aborting\n");
			return -1;
		}
	}
	g_skip_init = 0;

	return 0;
}
char *
strptime(const char *buf, const char *fmt, struct tm *tm)
{
	char c;
	const char *bp;
	size_t len = 0;
	int alt_format, i, split_year = 0;

	bp = buf;

	while ((c = *fmt) != '\0') {
		/* Clear `alternate' modifier prior to new conversion. */
		alt_format = 0;

		/* Eat up white-space. */
		if (isspace(c)) {
			while (isspace(*bp))
				bp++;

			fmt++;
			continue;
		}

		if ((c = *fmt++) != '%')
			goto literal;


	again:		switch (c = *fmt++) {
	case '%':	/* "%%" is converted to "%". */
		literal :
		if (c != *bp++)
			return (0);
		break;

		/*
		* "Alternative" modifiers. Just set the appropriate flag
		* and start over again.
		*/
	case 'E':	/* "%E?" alternative conversion modifier. */
		LEGAL_ALT(0);
		alt_format |= ALT_E;
		goto again;

	case 'O':	/* "%O?" alternative conversion modifier. */
		LEGAL_ALT(0);
		alt_format |= ALT_O;
		goto again;

		/*
		* "Complex" conversion rules, implemented through recursion.
		*/
	case 'c':	/* Date and time, using the locale's format. */
		LEGAL_ALT(ALT_E);
		if (!(bp = strptime(bp, "%x %X", tm)))
			return (0);
		break;

	case 'D':	/* The date as "%m/%d/%y". */
		LEGAL_ALT(0);
		if (!(bp = strptime(bp, "%m/%d/%y", tm)))
			return (0);
		break;

	case 'R':	/* The time as "%H:%M". */
		LEGAL_ALT(0);
		if (!(bp = strptime(bp, "%H:%M", tm)))
			return (0);
		break;

	case 'r':	/* The time in 12-hour clock representation. */
		LEGAL_ALT(0);
		if (!(bp = strptime(bp, "%I:%M:%S %p", tm)))
			return (0);
		break;

	case 'T':	/* The time as "%H:%M:%S". */
		LEGAL_ALT(0);
		if (!(bp = strptime(bp, "%H:%M:%S", tm)))
			return (0);
		break;

	case 'X':	/* The time, using the locale's format. */
		LEGAL_ALT(ALT_E);
		if (!(bp = strptime(bp, "%H:%M:%S", tm)))
			return (0);
		break;

	case 'x':	/* The date, using the locale's format. */
		LEGAL_ALT(ALT_E);
		if (!(bp = strptime(bp, "%m/%d/%y", tm)))
			return (0);
		break;

		/*
		* "Elementary" conversion rules.
		*/
	case 'A':	/* The day of week, using the locale's form. */
	case 'a':
		LEGAL_ALT(0);
		for (i = 0; i < 7; i++) {
			/* Full name. */
			len = strlen(day[i]);
			if (strncasecmp(day[i], bp, len) == 0)
				break;

			/* Abbreviated name. */
			len = strlen(abday[i]);
			if (strncasecmp(abday[i], bp, len) == 0)
				break;
		}

		/* Nothing matched. */
		if (i == 7)
			return (0);

		tm->tm_wday = i;
		bp += len;
		break;

	case 'B':	/* The month, using the locale's form. */
	case 'b':
	case 'h':
		LEGAL_ALT(0);
		for (i = 0; i < 12; i++) {
			/* Full name. */
			len = strlen(mon[i]);
			if (strncasecmp(mon[i], bp, len) == 0)
				break;

			/* Abbreviated name. */
			len = strlen(abmon[i]);
			if (strncasecmp(abmon[i], bp, len) == 0)
				break;
		}

		/* Nothing matched. */
		if (i == 12)
			return (0);

		tm->tm_mon = i;
		bp += len;
		break;

	case 'C':	/* The century number. */
		LEGAL_ALT(ALT_E);
		if (!(conv_num(&bp, &i, 0, 99)))
			return (0);

		if (split_year) {
			tm->tm_year = (tm->tm_year % 100) + (i * 100);
		}
		else {
			tm->tm_year = i * 100;
			split_year = 1;
		}
		break;

	case 'd':	/* The day of month. */
	case 'e':
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &tm->tm_mday, 1, 31)))
			return (0);
		break;

	case 'k':	/* The hour (24-hour clock representation). */
		LEGAL_ALT(0);
		/* FALLTHROUGH */
	case 'H':
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &tm->tm_hour, 0, 23)))
			return (0);
		break;

	case 'l':	/* The hour (12-hour clock representation). */
		LEGAL_ALT(0);
		/* FALLTHROUGH */
	case 'I':
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &tm->tm_hour, 1, 12)))
			return (0);
		if (tm->tm_hour == 12)
			tm->tm_hour = 0;
		break;

	case 'j':	/* The day of year. */
		LEGAL_ALT(0);
		if (!(conv_num(&bp, &i, 1, 366)))
			return (0);
		tm->tm_yday = i - 1;
		break;

	case 'M':	/* The minute. */
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &tm->tm_min, 0, 59)))
			return (0);
		break;

	case 'm':	/* The month. */
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &i, 1, 12)))
			return (0);
		tm->tm_mon = i - 1;
		break;

	case 'p':	/* The locale's equivalent of AM/PM. */
		LEGAL_ALT(0);
		/* AM? */
		if (strcasecmp(am_pm[0], bp) == 0) {
			if (tm->tm_hour > 11)
				return (0);

			bp += strlen(am_pm[0]);
			break;
		}
		/* PM? */
		else if (strcasecmp(am_pm[1], bp) == 0) {
			if (tm->tm_hour > 11)
				return (0);

			tm->tm_hour += 12;
			bp += strlen(am_pm[1]);
			break;
		}

		/* Nothing matched. */
		return (0);

	case 'S':	/* The seconds. */
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &tm->tm_sec, 0, 61)))
			return (0);
		break;

	case 'U':	/* The week of year, beginning on sunday. */
	case 'W':	/* The week of year, beginning on monday. */
		LEGAL_ALT(ALT_O);
		/*
		* XXX This is bogus, as we can not assume any valid
		* information present in the tm structure at this
		* point to calculate a real value, so just check the
		* range for now.
		*/
		if (!(conv_num(&bp, &i, 0, 53)))
			return (0);
		break;

	case 'w':	/* The day of week, beginning on sunday. */
		LEGAL_ALT(ALT_O);
		if (!(conv_num(&bp, &tm->tm_wday, 0, 6)))
			return (0);
		break;

	case 'Y':	/* The year. */
		LEGAL_ALT(ALT_E);
		if (!(conv_num(&bp, &i, 0, 9999)))
			return (0);

		tm->tm_year = i - TM_YEAR_BASE;
		break;

	case 'y':	/* The year within 100 years of the epoch. */
		LEGAL_ALT(ALT_E | ALT_O);
		if (!(conv_num(&bp, &i, 0, 99)))
			return (0);

		if (split_year) {
			tm->tm_year = ((tm->tm_year / 100) * 100) + i;
			break;
		}
		split_year = 1;
		if (i <= 68)
			tm->tm_year = i + 2000 - TM_YEAR_BASE;
		else
			tm->tm_year = i + 1900 - TM_YEAR_BASE;
		break;

		/*
		* Miscellaneous conversions.
		*/
	case 'n':	/* Any kind of white-space. */
	case 't':
		LEGAL_ALT(0);
		while (isspace(*bp))
			bp++;
		break;


	default:	/* Unknown/unsupported conversion. */
		return (0);
	}


	}

	/* LINTED functional specification */
	return ((char *)bp);
}


static int
conv_num(const char **buf, int *dest, int llim, int ulim)
{
	int result = 0;

	/* The limit also determines the number of valid digits. */
	int rulim = ulim;

	if (**buf < '0' || **buf > '9')
		return (0);

	do {
		result *= 10;
		result += *(*buf)++ - '0';
		rulim /= 10;
	} while ((result * 10 <= ulim) && rulim && **buf >= '0' && **buf <= '9');

	if (result < llim || result > ulim)
		return (0);

	*dest = result;
	return (1);
}