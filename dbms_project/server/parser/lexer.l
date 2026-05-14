%option noyywrap noinput nounput yylineno

%{
#include <cstdlib>
#include <cstring>
#include <string>
#include <stdio.h>

#include "grammar.tab.h"

static char* copy_text(const char* text) {
    const std::size_t length = std::strlen(text);
    char* copy = static_cast<char*>(std::malloc(length + 1));
    if (copy == nullptr) {
        return nullptr;
    }
    std::memcpy(copy, text, length + 1);
    return copy;
}

static char* copy_sql_string_literal(const char* text) {
    const std::string input(text);
    std::string output;

    for (std::size_t i = 1; i + 1 < input.size(); ++i) {
        if (input[i] == '\\' && i + 1 < input.size() - 1) {
            ++i;
        }
        output.push_back(input[i]);
    }

    return copy_text(output.c_str());
}
%}

WS              [ \t\r\n]+
DIGIT           [0-9]
ID_START        [A-Za-z_]
ID_CONT         [A-Za-z0-9_]
ID_TOKEN        {ID_START}{ID_CONT}*
INT_LITERAL_RE  {DIGIT}+
STRING_LITERAL_RE \'([^\\\n']|\\.)*\'

KW_SELECT       [Ss][Ee][Ll][Ee][Cc][Tt]
KW_USE          [Uu][Ss][Ee]
KW_FROM         [Ff][Rr][Oo][Mm]
KW_WHERE        [Ww][Hh][Ee][Rr][Ee]
KW_INSERT       [Ii][Nn][Ss][Ee][Rr][Tt]
KW_INTO         [Ii][Nn][Tt][Oo]
KW_VALUE        [Vv][Aa][Ll][Uu][Ee]
KW_VALUES       [Vv][Aa][Ll][Uu][Ee][Ss]
KW_CREATE       [Cc][Rr][Ee][Aa][Tt][Ee]
KW_DROP         [Dd][Rr][Oo][Pp]
KW_TABLE        [Tt][Aa][Bb][Ll][Ee]
KW_DATABASE     [Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee]
KW_UPDATE       [Uu][Pp][Dd][Aa][Tt][Ee]
KW_SET          [Ss][Ee][Tt]
KW_DELETE       [Dd][Ee][Ll][Ee][Tt][Ee]
KW_AS           [Aa][Ss]
KW_BETWEEN      [Bb][Ee][Tt][Ww][Ee][Ee][Nn]
KW_LIKE         [Ll][Ii][Kk][Ee]
KW_AND          [Aa][Nn][Dd]
KW_OR           [Oo][Rr]
KW_NOT          [Nn][Oo][Tt]
KW_NULL         [Nn][Uu][Ll][Ll]
KW_INDEXED      [Ii][Nn][Dd][Ee][Xx][Ee][Dd]
KW_INT          [Ii][Nn][Tt]
KW_TEXT         [Tt][Ee][Xx][Tt]

%%

{WS}            { /* skip whitespace */ }

{KW_SELECT}     { return SELECT; }
{KW_USE}        { return USE; }
{KW_FROM}       { return FROM; }
{KW_WHERE}      { return WHERE; }
{KW_INSERT}     { return INSERT; }
{KW_INTO}       { return INTO; }
{KW_VALUE}      { return VALUES; }
{KW_VALUES}     { return VALUES; }
{KW_CREATE}     { return CREATE; }
{KW_DROP}       { return DROP; }
{KW_TABLE}      { return TABLE; }
{KW_DATABASE}   { return DATABASE; }
{KW_UPDATE}     { return UPDATE; }
{KW_SET}        { return SET; }
{KW_DELETE}     { return DELETE; }
{KW_AS}         { return AS; }
{KW_BETWEEN}    { return BETWEEN; }
{KW_LIKE}       { return LIKE; }
{KW_AND}        { return AND; }
{KW_OR}         { return OR; }
{KW_NOT}        { return NOT; }
{KW_NULL}       { return NULL_T; }
{KW_INDEXED}    { return INDEXED; }
{KW_INT}        { return INT; }
{KW_TEXT}       { return TEXT; }

"*"             { return '*'; }
","             { return ','; }
"("             { return '('; }
")"             { return ')'; }
";"             { return ';'; }
"=="            { return EQEQ; }
"!="            { return NEQ; }
">="            { return GEQ; }
"<="            { return LEQ; }
"="             { return '='; }
">"             { return '>'; }
"<"             { return '<'; }

{INT_LITERAL_RE}      {
                          yylval.int_value = std::atoi(yytext);
                          return INT_LITERAL;
                      }
{STRING_LITERAL_RE}   {
                          yylval.str = copy_sql_string_literal(yytext);
                          return STRING_LITERAL;
                      }
{ID_TOKEN}            {
                          yylval.str = copy_text(yytext);
                          return ID;
                      }

\'([^\\\n']|\\.)*$ {
                    fprintf(stderr,
                            "Lexer error (line %d): unterminated string literal: %s\n",
                            yylineno,
                            yytext);
                }

.               {
                    fprintf(stderr,
                            "Lexer error (line %d): unknown symbol '%s'\n",
                            yylineno,
                            yytext);
                }

%%
