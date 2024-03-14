#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#define COLUMN_USER_SIZE 32
#define COLUMN_EMAIL_SIZE 256
#define attribute_size(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
#define TABLE_MAX_PAGES 100

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef struct
{
    uint32_t id;
    char user[COLUMN_USER_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID
} PrepareResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct
{
    StatementType type;
    Row row_insert;
} Statement;

const uint32_t ID_SIZE = attribute_size(Row, id);
const uint32_t USER_SIZE = attribute_size(Row, user);
const uint32_t EMAIL_SIZE = attribute_size(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USER_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USER_OFFSET + USER_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USER_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct
{
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
    uint32_t rows_num;
    Pager *pager;
} Table;

void serialize_row(Row *src, void *dest)
{
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dest + USER_OFFSET, &(src->user), USER_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void *src, Row *dest)
{
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->user), src + USER_OFFSET, USER_SIZE);
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

void *get_page(Pager *pager, uint32_t page_num)
{
    if (page_num > TABLE_MAX_PAGES)
    {
        printf("Page number out of bounds!\n");
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL)
    {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if (pager->file_length % PAGE_SIZE)
            num_pages += 1;

        if (page_num <= num_pages)
        {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1)
            {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

void *row_slot(Table *table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

InputBuffer *new_input_buffer()
{

    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void read_input(InputBuffer *input_buffer)
{

    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0)
    {
        printf("Input error\n");
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void clear_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_cmd() { printf("db > "); }

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        clear_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_str = strtok(NULL, " ");
    char *user = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_str == NULL || user == NULL || email == NULL)
        return PREPARE_SYNTAX_ERROR;

    int id = atoi(id_str);

    if (id < 0)
        return PREPARE_NEGATIVE_ID;

    if (strlen(user) > COLUMN_USER_SIZE)
        return PREPARE_STRING_TOO_LONG;

    if (strlen(email) > COLUMN_EMAIL_SIZE)
        return PREPARE_STRING_TOO_LONG;

    statement->row_insert.id = id;
    strcpy(statement->row_insert.user, user);
    strcpy(statement->row_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
        return prepare_insert(input_buffer, statement);

    if (strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void print_row(Row *row)
{
    printf("%d %s %s\n", row->id, row->user, row->email);
}

ExecuteResult execute_insert(Statement *statement, Table *table)
{
    if (table->rows_num >= TABLE_MAX_ROWS)
        return EXECUTE_TABLE_FULL;

    Row *row_insert = &(statement->row_insert);

    serialize_row(row_insert, row_slot(table, table->rows_num));
    table->rows_num += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table)
{
    Row row;
    for (uint32_t i = 0; i < table->rows_num; i++)
    {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table)
{
    if (statement->type == STATEMENT_INSERT)
        return execute_insert(statement, table);

    if (statement->type == STATEMENT_SELECT)
        return execute_select(statement, table);
}

Pager *pager_open(const char *filename)
{

    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        printf("Unable to open file!\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
        pager->pages[i] = NULL;

    return pager;
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
    if (pager->pages[page_num] == NULL)
    {
        printf("Tried to flush null page!\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1)
    {
        printf("Error seeking: %d!\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1)
    {
        printf("Error writing: %d!\n", errno);
        exit(EXIT_FAILURE);
    }
}

Table *db_open(const char *filename)
{
    Pager *pager = pager_open(filename);
    uint32_t rows_num = pager->file_length / ROW_SIZE;
    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->rows_num = rows_num;
    return table;
}

void db_close(Table *table)
{
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->rows_num / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++)
    {
        if (pager->pages[i] == NULL)
            continue;
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    uint32_t num_additional_rows = table->rows_num % ROWS_PER_PAGE;

    if (num_additional_rows > 0)
    {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL)
        {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1)
    {
        printf("Error closing db file!\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void *page = pager->pages[i];
        if (page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printf("Must supply database filename!\n");

        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    Table *table = db_open(filename);

    InputBuffer *input_buffer = new_input_buffer();
    while (true)
    {
        print_cmd();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer, table))
            {
            case META_COMMAND_SUCCESS:
                continue;
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Unrecognized Meta-command '%s' !\n", input_buffer->buffer);
                continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("Command '%s' not found !\n", input_buffer->buffer);
            continue;
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error !\n");
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("String is too long!\n");
            continue;
        case PREPARE_NEGATIVE_ID:
            printf("ID must be positive!\n");
            continue;
        }
        switch (execute_statement(&statement, table))
        {
        case EXECUTE_SUCCESS:
            printf("Command Executed!\n");
            continue;
        case EXECUTE_TABLE_FULL:
            printf("Error: Table full!\n");
            break;
        }
    }
}