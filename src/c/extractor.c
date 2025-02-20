#include <stdio.h>
#include <string.h>

//let script_regex = RegexBuilder::new(r"<script.*?>(.*?)<\/script>")
//    .dot_matches_new_line(true)
//    .build()
//    .unwrap();
//
//let schema_regex = RegexBuilder::new(r#"\{.{0,1000}?(schema|("@type": "Recipe")).{0,1000}?@type.*\}"#)
//    .dot_matches_new_line(true)
//    .build()
//    .unwrap();

int check_prefix(char* actual, char* expected) {
    int i = 0;
    while (expected[i] != 0 && actual[i] != 0 && expected[i] == actual[i]) {
        i++;
    }
    printf("%c\n", *actual);
    return expected[i] == 0;
}

void test_check_prefix() {
    printf("test_check_prefix 1: %i\n", check_prefix("bruh", "br") == 1);
    printf("test_check_prefix 2: %i\n", check_prefix("bbruh", "br") == 0);
    printf("test_check_prefix 3: %i\n", check_prefix("b", "br") == 0);
}

// State 0: Looking for a <script.*>
int handle_state_0(char** current, char** match_start) {
    while (**current != 0 && **current != '<') {
        (*current)++;
    }

    if (**current == 0) {
        return -1;
    }

    if (**current == '<' && check_prefix(*current, "<script>")) {
        *current += strlen("<script>");
        *match_start = *current;
        return 1;
    }

    return 0;
}

void test_handle_state_0() {
    char* string = "";
    char* current = string;
    char* match_start = NULL;
    printf("test_handle_state_0 1: %i\n", handle_state_0(&current, &match_start) == -1);

    string = "bruh";
    current = string;
    match_start = NULL;
    printf("test_handle_state_0 2: %i\n", handle_state_0(&current, &match_start) == -1);

    string = "aijisj\n<script>xyz";
    current = string;
    match_start = NULL;
    printf("test_handle_state_0 3: %i\n", handle_state_0(&current, &match_start) == 1);
    printf("test_handle_state_0 3: %i\n", match_start != NULL && *match_start == 'x');
    printf("test_handle_state_0 3: %i\n", *current == 'x');
}

// State 1: Looking for a 'schema' or ' </script>
int handle_state_1(char** current, char** match_start) {

}

char* extract(char* in) {
    char* current = in;
    char* match_start = NULL;
    int state = 0;

    while (1) {
        switch (state) {
            case 0: state = handle_state_0(&current, &match_start); break;
        }

        if (state == -1) {
            return NULL;
        }
    }
}

int main() {
    test_check_prefix();
    test_handle_state_0();
    return 1;
}

