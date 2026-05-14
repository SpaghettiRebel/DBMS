%code requires {
#include <string>
#include <vector>

#include "QueryPlan.h"

struct AssignmentList {
    std::vector<std::string> columns;
    std::vector<Value> values;
};
}

%{
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <stdio.h>

#include "QueryPlan.h"

QueryPlan parsed_query_plan;

void reset_parsed_query_plan();
int yylex(void);
void yyerror(const char* s);

namespace {

constexpr int COLUMN_FLAG_NOT_NULL = 1;
constexpr int COLUMN_FLAG_INDEXED = 2;

std::string take_string(char* value) {
    std::string result = value != nullptr ? value : "";
    std::free(value);
    return result;
}

Value make_int_value(int value) {
    Value result{};
    result.is_null = false;
    result.data = value;
    return result;
}

Value make_string_value(char* value) {
    Value result{};
    result.is_null = false;
    result.data = take_string(value);
    return result;
}

ConditionNode* make_condition(char* column_name, OpType op, Value* value) {
    auto* condition = new ConditionNode{};
    condition->is_leaf = true;
    condition->left_column = take_string(column_name);
    condition->op = op;
    condition->right_value = std::move(*value);
    delete value;
    return condition;
}

ConditionNode* make_between_condition(char* column_name, Value* lower, Value* upper) {
    auto* condition = new ConditionNode{};
    condition->is_leaf = true;
    condition->left_column = take_string(column_name);
    condition->op = OpType::BETWEEN;
    condition->right_value = std::move(*lower);
    condition->right_value_between = std::move(*upper);
    delete lower;
    delete upper;
    return condition;
}

ConditionNode* make_logical_condition(ConditionNode* left, LogicalOpType op, ConditionNode* right) {
    auto* condition = new ConditionNode{};
    condition->is_leaf = false;
    condition->logical_op = op;
    condition->left_child.reset(left);
    condition->right_child.reset(right);
    return condition;
}

SelectTarget make_select_target(char* column_name, char* alias) {
    SelectTarget target{};
    target.column_name = take_string(column_name);
    target.aggregate = AggregateType::NONE;
    if (alias != nullptr) {
        target.alias = take_string(alias);
    }
    return target;
}

}  // namespace
%}

%union {
    char* str;
    int int_value;
    OpType op_type;
    Value* value;
    std::vector<std::string>* string_list;
    std::vector<Value>* value_list;
    std::vector<std::vector<Value>>* value_rows;
    ColumnDef* column;
    std::vector<ColumnDef>* column_list;
    ConditionNode* condition;
    SelectTarget* select_target;
    std::vector<SelectTarget>* select_target_list;
    AssignmentList* assignment_list;
}

%token SELECT USE FROM WHERE INSERT INTO VALUES CREATE TABLE DATABASE INT TEXT
%token DROP UPDATE SET DELETE AS BETWEEN LIKE AND OR NOT NULL_T INDEXED
%token EQEQ NEQ LEQ GEQ
%token <str> ID STRING_LITERAL
%token <int_value> INT_LITERAL

%type <select_target_list> select_list select_target_list
%type <select_target> select_target
%type <string_list> id_list insert_columns
%type <value> literal
%type <value_list> value_list
%type <value_rows> value_rows
%type <column> column_def
%type <column_list> column_defs
%type <condition> opt_where condition predicate
%type <int_value> column_type column_modifiers
%type <op_type> comparison_operator
%type <assignment_list> assignment_list

%destructor { std::free($$); } <str>
%destructor { delete $$; } <value> <string_list> <value_list> <value_rows> <column> <column_list> <condition> <select_target> <select_target_list> <assignment_list>

%left OR
%left AND

%start input

%%

input
    : statement ';'
    | statement
    ;

statement
    : select_stmt
    | use_stmt
    | insert_stmt
    | create_stmt
    | drop_stmt
    | update_stmt
    | delete_stmt
    ;

use_stmt
    : USE ID
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::USE_DATABASE;
          parsed_query_plan.database_name = take_string($2);
      }
    ;

select_stmt
    : SELECT select_list FROM ID opt_where
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::SELECT;
          parsed_query_plan.table_name = take_string($4);
          parsed_query_plan.select_targets = std::move(*$2);
          parsed_query_plan.where_clause.reset($5);
          delete $2;
      }
    ;

select_list
    : '*'
      {
          $$ = new std::vector<SelectTarget>();
          SelectTarget target{};
          target.column_name = "*";
          target.aggregate = AggregateType::NONE;
          $$->push_back(std::move(target));
      }
    | select_target_list
      {
          $$ = $1;
      }
    ;

select_target_list
    : select_target
      {
          $$ = new std::vector<SelectTarget>();
          $$->push_back(std::move(*$1));
          delete $1;
      }
    | select_target_list ',' select_target
      {
          $1->push_back(std::move(*$3));
          delete $3;
          $$ = $1;
      }
    ;

select_target
    : ID
      {
          $$ = new SelectTarget(make_select_target($1, nullptr));
      }
    | ID AS ID
      {
          $$ = new SelectTarget(make_select_target($1, $3));
      }
    ;

opt_where
    : /* empty */
      {
          $$ = nullptr;
      }
    | WHERE condition
      {
          $$ = $2;
      }
    ;

condition
    : predicate
      {
          $$ = $1;
      }
    | condition AND condition
      {
          $$ = make_logical_condition($1, LogicalOpType::AND, $3);
      }
    | condition OR condition
      {
          $$ = make_logical_condition($1, LogicalOpType::OR, $3);
      }
    | '(' condition ')'
      {
          $$ = $2;
      }
    ;

predicate
    : ID comparison_operator literal
      {
          $$ = make_condition($1, $2, $3);
      }
    | ID BETWEEN literal AND literal
      {
          $$ = make_between_condition($1, $3, $5);
      }
    | ID LIKE literal
      {
          $$ = make_condition($1, OpType::LIKE, $3);
      }
    ;

comparison_operator
    : '='
      {
          $$ = OpType::EQ;
      }
    | EQEQ
      {
          $$ = OpType::EQ;
      }
    | NEQ
      {
          $$ = OpType::NEQ;
      }
    | '>'
      {
          $$ = OpType::GREATER;
      }
    | '<'
      {
          $$ = OpType::LESS;
      }
    | GEQ
      {
          $$ = OpType::GEQ;
      }
    | LEQ
      {
          $$ = OpType::LEQ;
      }
    ;

literal
    : INT_LITERAL
      {
          $$ = new Value(make_int_value($1));
      }
    | STRING_LITERAL
      {
          $$ = new Value(make_string_value($1));
      }
    ;

insert_stmt
    : INSERT INTO ID insert_columns VALUES value_rows
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::INSERT;
          parsed_query_plan.table_name = take_string($3);
          parsed_query_plan.target_columns = std::move(*$4);
          parsed_query_plan.value_rows = std::move(*$6);
          parsed_query_plan.values.clear();
          if (!parsed_query_plan.value_rows.empty()) {
              parsed_query_plan.values = parsed_query_plan.value_rows.front();
          }
          delete $4;
          delete $6;
      }
    ;

insert_columns
    : /* empty */
      {
          $$ = new std::vector<std::string>();
      }
    | '(' id_list ')'
      {
          $$ = $2;
      }
    ;

id_list
    : ID
      {
          $$ = new std::vector<std::string>();
          $$->push_back(take_string($1));
      }
    | id_list ',' ID
      {
          $1->push_back(take_string($3));
          $$ = $1;
      }
    ;

value_rows
    : '(' value_list ')'
      {
          $$ = new std::vector<std::vector<Value>>();
          $$->push_back(std::move(*$2));
          delete $2;
      }
    | value_rows ',' '(' value_list ')'
      {
          $1->push_back(std::move(*$4));
          delete $4;
          $$ = $1;
      }
    ;

value_list
    : literal
      {
          $$ = new std::vector<Value>();
          $$->push_back(std::move(*$1));
          delete $1;
      }
    | value_list ',' literal
      {
          $1->push_back(std::move(*$3));
          delete $3;
          $$ = $1;
      }
    ;

create_stmt
    : CREATE DATABASE ID
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::CREATE_DATABASE;
          parsed_query_plan.database_name = take_string($3);
      }
    | CREATE TABLE ID '(' column_defs ')'
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::CREATE_TABLE;
          parsed_query_plan.table_name = take_string($3);
          parsed_query_plan.columns = std::move(*$5);
          delete $5;
      }
    ;

drop_stmt
    : DROP DATABASE ID
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::DROP_DATABASE;
          parsed_query_plan.database_name = take_string($3);
      }
    | DROP TABLE ID
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::DROP_TABLE;
          parsed_query_plan.table_name = take_string($3);
      }
    ;

update_stmt
    : UPDATE ID SET assignment_list WHERE condition
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::UPDATE;
          parsed_query_plan.table_name = take_string($2);
          parsed_query_plan.target_columns = std::move($4->columns);
          parsed_query_plan.values = std::move($4->values);
          parsed_query_plan.where_clause.reset($6);
          delete $4;
      }
    ;

assignment_list
    : ID '=' literal
      {
          $$ = new AssignmentList();
          $$->columns.push_back(take_string($1));
          $$->values.push_back(std::move(*$3));
          delete $3;
      }
    | assignment_list ',' ID '=' literal
      {
          $1->columns.push_back(take_string($3));
          $1->values.push_back(std::move(*$5));
          delete $5;
          $$ = $1;
      }
    ;

delete_stmt
    : DELETE FROM ID WHERE condition
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::DELETE;
          parsed_query_plan.table_name = take_string($3);
          parsed_query_plan.where_clause.reset($5);
      }
    ;

column_defs
    : column_def
      {
          $$ = new std::vector<ColumnDef>();
          $$->push_back(std::move(*$1));
          delete $1;
      }
    | column_defs ',' column_def
      {
          $1->push_back(std::move(*$3));
          delete $3;
          $$ = $1;
      }
    ;

column_type
    : INT
      {
          $$ = 0;
      }
    | TEXT
      {
          $$ = 1;
      }
    ;

column_modifiers
    : /* empty */
      {
          $$ = 0;
      }
    | column_modifiers NOT NULL_T
      {
          $$ = $1 | COLUMN_FLAG_NOT_NULL;
      }
    | column_modifiers INDEXED
      {
          $$ = $1 | COLUMN_FLAG_INDEXED;
      }
    ;

column_def
    : ID column_type column_modifiers
      {
          $$ = new ColumnDef{};
          $$->name = take_string($1);
          $$->type = ($2 == 0 ? DataType::INT : DataType::STRING);
          $$->is_not_null = (($3 & COLUMN_FLAG_NOT_NULL) != 0);
          $$->is_indexed = (($3 & COLUMN_FLAG_INDEXED) != 0);
      }
    ;

%%

void reset_parsed_query_plan() {
    parsed_query_plan = QueryPlan{};
}

void yyerror(const char* s) {
    fprintf(stderr, "Parser error: %s\n", s);
}
