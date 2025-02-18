import subprocess
import unittest
import os
import tempfile

def run_script(commands, db_filename):
    raw_output = None
    with subprocess.Popen(["./main", db_filename], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        for command in commands:
            process.stdin.write(command + '\n')
        process.stdin.close()
        raw_output = process.stdout.read()
    return raw_output.split("\n")


class TestDatabase(unittest.TestCase):
    def setUp(self):
        """Create a temporary database file before each test."""
        self.db_file = tempfile.NamedTemporaryFile(delete=False)
        self.db_file.close()

    def tearDown(self):
        """Remove the temporary database file after each test."""
        os.remove(self.db_file.name)

    def test_inserts_and_retrieves_row(self):
        result = run_script([
            "insert 1 user1 mail1@example.com",
            "select",
            ".exit",
        ], self.db_file.name)
        expected_output = [
            "db > Command Executed!",
            "db > 1 user1 mail1@example.com",
            "Command Executed!",
            "db > ",
        ]
        self.assertListEqual(result, expected_output)

    def test_error_table_is_full(self):
        script = ["insert {} user{} mail{}@example.com".format(i, i, i) for i in range(1, 1402)]
        script.append(".exit")
        result = run_script(script, self.db_file.name)
        self.assertEqual(result[-2], "db > Error: Table full!")

    def test_too_long_user_and_too_long_email(self):
        long_user = "a" * 33
        long_mail = "a" * 257
        script = [
            f"insert 1 {long_user} {long_mail}",
            "select",
            ".exit",
        ]
        result = run_script(script, self.db_file.name)
        expected_output = [
            "db > String is too long!",
            "db > Command Executed!",
            "db > ",
        ]
        self.assertListEqual(result, expected_output)

    def test_allows_inserting_strings_that_are_the_maximum_length(self):
        long_username = "a" * 32
        long_email = "a" * 256
        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit",
        ]
        result = run_script(script, self.db_file.name)
        expected_output = [
            "db > Command Executed!",
            f"db > 1 {long_username} {long_email}",
            "Command Executed!",
            "db > ",
        ]
        self.assertListEqual(result, expected_output)

    def test_negative_id(self):
        result = run_script([
            "insert -1 user1 mail1@example.com",
            "select",
            ".exit",
        ], self.db_file.name)
        expected_output = [
            "db > ID must be positive!",
            "db > Command Executed!",
            "db > ",
        ]
        self.assertListEqual(result, expected_output)

    def test_data_is_saved_after(self):
        script1 = [
            "insert 1 user email@example.com",
            ".exit",
        ]
        result1 = run_script(script1, self.db_file.name)
        expected_output1 = [
            "db > Command Executed!",
            "db > ",
        ]
        self.assertListEqual(result1, expected_output1)
        
        result2 = run_script([
            "select",
            ".exit"
        ], self.db_file.name)
        expected_output2 = [
            "db > 1 user email@example.com",
            "Command Executed!",
            "db > "
        ]
        self.assertListEqual(result2, expected_output2)


if __name__ == "__main__":
    unittest.main()

