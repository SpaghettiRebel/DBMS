%code requires {
#include <string>
#include <vector>

#include "QueryPlan.h"
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

void apply_select_targets(const std::vector<std::string>& columns) {
    parsed_query_plan.select_targets.clear();
    for (const auto& column : columns) {
        SelectTarget target{};
        target.column_name = column;
        target.aggregate = AggregateType::NONE;
        parsed_query_plan.select_targets.push_back(std::move(target));
    }
}

}  // namespace
%}

%union {
    char* str;
    int int_value;
    Value* value;
    std::vector<std::string>* string_list;
    std::vector<Value>* value_list;
    ColumnDef* column;
    std::vector<ColumnDef>* column_list;
    ConditionNode* condition;
}

%token SELECT USE FROM WHERE INSERT INTO VALUES CREATE TABLE DATABASE INT TEXT
%token <str> ID STRING_LITERAL
%token <int_value> INT_LITERAL

%type <string_list> id_list select_list insert_columns
%type <value> literal
%type <value_list> value_list
%type <column> column_def
%type <column_list> column_defs
%type <condition> opt_where condition

%destructor { std::free($$); } <str>
%destructor { delete $$; } <value> <string_list> <value_list> <column> <column_list> <condition>

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
          apply_select_targets(*$2);
          parsed_query_plan.where_clause.reset($5);
          delete $2;
      }
    ;

select_list
    : '*'
      {
          $$ = new std::vector<std::string>();
      }
    | id_list
      {
          $$ = $1;
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
    : ID '=' literal
      {
          $$ = make_condition($1, OpType::EQ, $3);
      }
    | ID '>' literal
      {
          $$ = make_condition($1, OpType::GREATER, $3);
      }
    | ID '<' literal
      {
          $$ = make_condition($1, OpType::LESS, $3);
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
    : INSERT INTO ID insert_columns VALUES '(' value_list ')'
      {
          reset_parsed_query_plan();
          parsed_query_plan.type = QueryType::INSERT;
          parsed_query_plan.table_name = take_string($3);
          parsed_query_plan.target_columns = std::move(*$4);
          parsed_query_plan.values = std::move(*$7);
          delete $4;
          delete $7;
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

column_def
    : ID INT
      {
          $$ = new ColumnDef{};
          $$->name = take_string($1);
          $$->type = DataType::INT;
      }
    | ID TEXT
      {
          $$ = new ColumnDef{};
          $$->name = take_string($1);
          $$->type = DataType::STRING;
      }
    ;

%%

void reset_parsed_query_plan() {
    parsed_query_plan = QueryPlan{};
}

void yyerror(const char* s) {
    fprintf(stderr, "Parser error: %s\n", s);
}
