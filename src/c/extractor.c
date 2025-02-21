#include <stdio.h>
#include <stdlib.h>
#include <string.h>


int check_prefix(char* actual, char* expected) {
    int i = 0;
    for (; expected[i] != 0 && actual[i] != 0 && expected[i] == actual[i]; i++);
    return expected[i] == 0;
}

void test_check_prefix() {
    printf("test_check_prefix 1: %i\n", check_prefix("bruh", "br") == 1);
    printf("test_check_prefix 2: %i\n", check_prefix("bbruh", "br") == 0);
    printf("test_check_prefix 3: %i\n", check_prefix("b", "br") == 0);
}

// State 0: Looking for a <script.*>
int handle_state_0(char** current, char** match_start) {
    char* string = "<script";
    
    while (!check_prefix(*current, string)) {
        if (**current == 0) {
            return -1;
        }
        (*current)++;
    }
    *current += strlen(string);

    while (**current != '>') {
        if (**current == 0) {
            return -1;
        }
        (*current)++;
    }
    (*current)++;

    *match_start = *current;
    return 1;
}

void test_handle_state_0() {
    {
        char string[] = "";
        char* current = string;
        char* match_start = NULL;
        printf("test_handle_state_0 1: %i\n", handle_state_0(&current, &match_start) == -1);
    }

    {
        char string[] = "bruh";
        char* current = string;
        char* match_start = NULL;
        printf("test_handle_state_0 2: %i\n", handle_state_0(&current, &match_start) == -1);
    }

    {
        char string[] = "aijisj\n<script>xyz";
        char* current = string;
        char* match_start = NULL;
        printf("test_handle_state_0 3: %i\n", handle_state_0(&current, &match_start) == 1);
        printf("test_handle_state_0 3: %i\n", match_start != NULL && *match_start == 'x');
        printf("test_handle_state_0 3: %i\n", *current == 'x');
    }

    {
        char string[] = "aijisj\n<script src='bruh'>xyz";
        char* current = string;
        char* match_start = NULL;
        printf("test_handle_state_0 4: %i\n", handle_state_0(&current, &match_start) == 1);
        printf("test_handle_state_0 4: %i\n", match_start != NULL && *match_start == 'x');
        printf("test_handle_state_0 4: %i\n", *current == 'x');
    }

    {
        char string[] = "aijisj\n<script src='bruh'xyz";
        char* current = string;
        char* match_start = NULL;
        printf("test_handle_state_0 5: %i\n", handle_state_0(&current, &match_start) == -1);
    }
}

// State 1: Looking for a 'schema' or '"@type": "Recipe"', and reset to state 0 on </script>
int handle_state_1(char** current) {
    char* string1 = "</script";
    char* string2 = "schema";
    char* string3 = "\"@type\": \"Recipe\"";

    while (!check_prefix(*current, string1) && !check_prefix(*current, string2) && !check_prefix(*current, string3)) {
        if (**current == 0) {
            return -1;
        }
        (*current)++;
    }

    if (check_prefix(*current, string1)) {
        return 0;
    }

    return 2;
}

void test_handle_state_1() {
    {
        char string[] = "";
        char* current = string;
        printf("test_handle_state_1 1: %i\n", handle_state_1(&current) == -1);
    }

    {
        char string[] = "bruh";
        char* current = string;
        printf("test_handle_state_1 2: %i\n", handle_state_1(&current) == -1);
    }

    {
        char string[] = "bruh thisn schema rurz</script>";
        char* current = string;
        printf("test_handle_state_1 3: %i\n", handle_state_1(&current) == 2);
        printf("test_handle_state_1 3: %i\n", *current == 's');
    }
}

// State 2: Looking for a </script>
int handle_state_2(char** current) {
    char* string = "</script";

    while (!check_prefix(*current, string)) {
        if (**current == 0) {
            return -1;
        }
        (*current)++;
    }

    (*current)--;

    return 3;
}

void test_handle_state_2() {
    {
        char string[] = "";
        char* current = string;
        printf("test_handle_state_2 1: %i\n", handle_state_2(&current) == -1);
    }

    {
        char string[] = "bruh";
        char* current = string;
        printf("test_handle_state_2 2: %i\n", handle_state_2(&current) == -1);
    }

    {
        char string[] = "bruh thisn schema rurz</script>";
        char* current = string;
        printf("test_handle_state_2 3: %i\n", handle_state_2(&current) == 3);
        printf("test_handle_state_2 3: %i\n", *current == 'z');
    }
}

char* extract(char* input) {
    char* current = input;
    char* match_start = NULL;
    int state = 0;

    while (1) {
        switch (state) {
            case 0: state = handle_state_0(&current, &match_start); break;
            case 1: state = handle_state_1(&current); break;
            case 2: state = handle_state_2(&current); break;
        }

        if (state == -1) {
            return NULL;
        }

        if (state == 3) {
            int size = (current - match_start) + 1;
            char* buffer = (char*) malloc(size);
            memcpy(buffer, match_start, size);
            return buffer;
        }
    }
}

void test_extract() {
    {
        char input[] = "<script> burn the naan schema</script>";
        char expected[] = " burn the naan schema";
        printf("test_extract: %i\n", !strcmp(extract(input), expected));
    }

}

//int main() {
//    test_check_prefix();
//    test_handle_state_0();
//    test_handle_state_1();
//    test_handle_state_2();
//    test_extract();
//    return 1;
//}

