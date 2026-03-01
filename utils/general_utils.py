import textwrap
from pathlib import Path
from typing import Callable, List, Optional

from dataset.gen_ceb.ceb_queries import ceb_templates
from dataset.gen_tpch.tpch_queries import tpc_h


def write_query_and_args_file(
    benchmark_name: str,
    gen_placeholders_fn: Callable,
    query_list: List[str],
    out_dir: str,
    use_fasttest_format: bool = True,
    storage_plan: Optional[str] = None,
) -> str:
    out_path = Path(out_dir)
    out_path.mkdir(exist_ok=True, parents=True)
    query_file = out_path / "queries.txt"
    args_file = out_path / "args_parser.hpp"
    example_code_cpp_file = out_path / "query_impl.cpp"

    if benchmark_name == "tpch":
        benchmark_queries = tpc_h
    elif benchmark_name == "ceb":
        benchmark_queries = ceb_templates
    else:
        raise ValueError(f"Unknown benchmark name: {benchmark_name}")

    # write the query file
    sql_template_list = []
    for q in query_list:
        sql_template_list.append(
            f"Query {q}:\n{benchmark_queries[f'Q{q}']}"
        )  # we always prefix with Q

    qf_string = "\n\n".join(sql_template_list)

    with open(query_file, "w") as qf:
        qf.write(qf_string)

    # write args file
    args_str, example_code = gen_args_str(
        query_list,
        use_fasttest_format=use_fasttest_format,
        gen_placeholders_fn=gen_placeholders_fn,
    )

    if use_fasttest_format:
        # write request args file
        with open(args_file, "w") as af:
            af.write(args_str)

        # insert example code into query_impl.cpp
        assert example_code_cpp_file.is_file(), (
            f"File not found: {example_code_cpp_file}"
        )
        with open(example_code_cpp_file, "r") as f:
            example_code_cpp = f.read()
        keyword = "//<<example parser call code>>"
        assert keyword in example_code_cpp, (
            f"Keyword '{keyword}' not found in {example_code_cpp_file}"
        )

        # replace the example code placeholder
        example_code_cpp = example_code_cpp.replace(keyword, example_code)

        # write back the modified query_impl.cpp
        with open(example_code_cpp_file, "w") as f:
            f.write(example_code_cpp)
    else:
        with open(args_file, "w") as af:
            af.write(args_str)
            af.write("\n")
            af.write(example_code)

    # written artifacts
    folder_context = f"{qf_string}\n\n{args_str}"

    # write storage plan to file if provided
    if storage_plan is not None:
        storage_plan_file = out_path / "storage_plan.txt"

        if storage_plan_file.exists():
            existing = storage_plan_file.read_text()
            assert existing == storage_plan, (
                f"Storage plan file already exists at {storage_plan_file} with different contents."
            )
        else:
            storage_plan_file.write_text(storage_plan)

        # append to context for versioning / snapshotting
        folder_context += f"\n\n{storage_plan}"

    return folder_context


def gen_args_str(
    query_ids: List[str],
    gen_placeholders_fn: Callable,
    use_fasttest_format: bool = True,
):
    out_str = ""
    out_str += "#pragma once\n\n"
    out_str += "#include <iomanip>\n"

    if use_fasttest_format:
        out_str += "#include <string>\n"
        out_str += "#include <sstream>\n"
        out_str += "#include <vector>\n"
        out_str += "\n"
        out_str += "struct QueryRequest {\n"
        out_str += "    std::string id;\n"
        out_str += "    std::string line;\n"
        out_str += "};\n\n"

        # A sample input line for the C++ parser would be (fields in declaration order):
        # ```
        # 1a 1990 ('movie', 'tv movie') "John Doe" ('m', '<<NULL>>')
        # ```

        # Which gets parsed into:
        # - `YEAR` = 1990
        # - `KIND` = ["movie", "tv movie"]
        # - `NAME` = "John Doe"
        # - `GENDER` = ["m", "<<NULL>>"]

        # Add helper function for parsing IN lists
        out_str += """// Helper function to parse IN list from tuple syntax: ('val1', 'val2', ...)
std::vector<std::string> parse_in_list(std::istringstream& iss) {
    std::vector<std::string> result;

    // Read opening parenthesis
    char c;
    iss >> std::ws >> c;
    if (c != '(') {
        std::ostringstream oss;
        oss << "Expected '(' at start of IN list, but got '"
            << c << "' (int=" << static_cast<int>(static_cast<unsigned char>(c)) << ")";
        throw std::runtime_error(oss.str());
    }

    bool first = true;
    while (iss >> std::ws) {
        // Check for closing parenthesis
        if (iss.peek() == ')') {
            iss.get(); // consume ')'
            break;
        }

        // Skip comma after first element
        if (!first) {
            iss >> std::ws >> c;
            if (c != ',') {
                throw std::runtime_error("Expected ',' between IN list elements");
            }
        }
        first = false;

        std::string value;
        iss >> std::ws;
        if (iss.peek() == '\'') {
            iss.get();
            while (iss) {
                const char ch = static_cast<char>(iss.get());
                if (!iss) break;
                if (ch == '\'') {
                    if (iss.peek() == '\'') {
                        iss.get();
                        value.push_back('\'');
                        continue;
                    }
                    break;
                }
                value.push_back(ch);
            }
        } else {
            while (iss && iss.peek() != ',' && iss.peek() != ')') {
                value.push_back(static_cast<char>(iss.get()));
            }
            const auto start = value.find_first_not_of(" \t\r\n");
            const auto end = value.find_last_not_of(" \t\r\n");
            if (start == std::string::npos) {
                value.clear();
            } else {
                value = value.substr(start, end - start + 1);
            }
        }

        result.push_back(value);
    }

    return result;
}

"""
    else:
        raise Exception(
            "Non-fasttest format is outdated and no longer supported. E.g. this IN list parsing is not ported back."
        )

    for q_id in query_ids:
        query_name = f"Q{q_id}"

        # gen a sample query to get placeholders
        placeholders_dict = gen_placeholders_fn(query_name=query_name)

        # gen the struct
        # struct Q5Args {
        #   std::string date_from;
        #   std::string date_to;
        #   std::string region;
        # };

        cpp_type_dict = {
            str: "std::string",
            int: "int",
            float: "float",
        }

        placeholder_str = []
        for placeholder, value in placeholders_dict.items():
            # Check if value is a serialized IN list (starts with '(')
            if isinstance(value, str) and value.startswith("("):
                placeholder_str.append(f"    std::vector<std::string> {placeholder};")
            else:
                placeholder_str.append(
                    f"    {cpp_type_dict[type(value)]} {placeholder};"
                )
        placeholder_str = "\n".join(placeholder_str)

        out_str += f"""
//{query_name}
struct {query_name}Args {{
{placeholder_str}
}};\n"""

        if use_fasttest_format:
            # gen the parse function
            # Q5Args parse_q5(const QueryRequest& request) {
            #     Q5Args args;
            #     std::isstringstream iss(request.line);
            #
            #     std::string qid;
            #     iss >> qid;  // consume query id
            #
            #     iss >> std::quoted(args.date_from) >> std::quoted(args.date_to) >> std::quoted(args.region);
            #
            #     return args;
            # }

            out_str += f"inline {query_name}Args parse_{query_name.lower()}(const QueryRequest& request) {{\n"
            out_str += f"    {query_name}Args args;\n"
            out_str += "    std::istringstream iss(request.line);\n"
            out_str += "\n"
            out_str += "    std::string qid;\n"
            out_str += "    if (!(iss >> qid)) {  // consume query id\n"
            out_str += (
                f'\t\tthrow std::runtime_error("Q{q_id}: failed to parse query id"); \n'
            )
            out_str += "    }\n"
            out_str += "\n"

        else:
            # gen the parse function
            # Q5Args parse_q5(std::istringstream& iss) {
            #     Q5Args args;
            #     iss >> std::quoted(args.date_from) >> std::quoted(args.date_to) >> std::quoted(args.region);
            #     return args;
            # }
            out_str += f"{query_name}Args parse_{query_name.lower()}(std::istringstream& iss) {{\n"
            out_str += f"    {query_name}Args args;\n"

        # gen the parsing code
        # Parse each field in the exact same order as serialization
        # IN lists must be parsed individually (can't use chained >> operator)

        for placeholder, value in placeholders_dict.items():
            is_in_list = isinstance(value, str) and value.startswith("(")

            if is_in_list:
                # Parse IN list using helper function
                out_str += f"\targs.{placeholder} = parse_in_list(iss);\n"
            elif isinstance(value, str):
                # Parse quoted string
                out_str += f"\tif (!(iss >> std::quoted(args.{placeholder}))) {{\n"
                out_str += f'\t\tthrow std::runtime_error("Q{q_id}: failed to parse {placeholder}");\n'
                out_str += "\t}\n"
            else:
                # Parse numeric value (int or float)
                out_str += f"\tif (!(iss >> args.{placeholder})) {{\n"
                out_str += f'\t\tthrow std::runtime_error("Q{q_id}: failed to parse {placeholder}");\n'
                out_str += "\t}\n"

        #     # add check for trailing input
        #     out_str += f"""\tiss >> std::ws;
        # char c;
        # if (iss >> c) {{
        #     throw std::runtime_error("Q{q_id}: trailing input detected");
        # }}
        # """
        out_str += """
    return args;
}
    """

    example_code = ""
    if use_fasttest_format:
        # switch case to call the parse function
        # for (const auto& req : requests) {
        #     switch (req.id) {
        #         case "1": {
        #             Q1Args args = parse_q1(req);
        #             run_q1(db, args);
        #             break;
        #         }
        #         case "2": {
        #             Q2Args args = parse_q2(req);
        #             run_q2(db, args);
        #             break;
        #         }
        #         // ...
        #         case "22": {
        #             Q22Args args = parse_q22(req);
        #             run_q22(db, args);
        #             break;
        #         }
        #         default:
        #             std::cerr << "Unknown query id: " << req.id << "\n";
        #             break;
        #     }
        # }

        example_code += "\n"
        example_code += "// Example code for how to use the parse functions together:\n"
        example_code += """//for (const auto& req : requests) {
//    switch (req.id) {"""

        def plot_case(i):
            return f"""
//        case "{i}": {{
//            Q{i}Args args = parse_q{i}(req); 
//            run_q{i}(db, args);
//            break;
//        }}"""

        for i in query_ids[:2]:
            example_code += plot_case(i)

        example_code += """
//        ..."""
        example_code += plot_case(query_ids[-1])
        example_code += """
//    }
//}
"""
    else:
        # switch case to call the parse function
        # std::string line;
        # while (std::getline(std::cin, line)) {
        #     std::istringstream iss(line);
        #
        #     std::string q;
        #     iss >> q;
        #
        #     switch (q) {
        #         case "13": parse_q13(iss); break;
        #         case "5":  parse_q5(iss);  break;
        #     }
        # }
        example_code += "\n"
        example_code += "// Example code for how to use the parse functions together:\n"
        example_code += """//std::string line;
//while (std::getline(std::cin, line)) {
//    std::istringstream iss(line);
//
//    std::string q;
//    iss >> q;
//
//    switch (q) {"""

        def plot_case(i):
            return f"""
//        case "{i}": {{
//            Q{i}Args args = parse_q{i}(iss); 
//            run_q{i}(args);
//            break;
//        }}"""

        for i in query_ids[:2]:
            example_code += plot_case(i)

        example_code += """
//        ..."""
        example_code += plot_case(query_ids[-1])

        example_code += """
//    }
//}
"""
    return out_str, example_code


def get_affinity_prompt(
    include_numa: bool = False,
    filename: str = "cpu_affinity.hpp",
) -> str:
    numa_section = ""
    if include_numa:
        assert not include_numa
        numa_section = textwrap.dedent("""\
            NUMA placement:
              Pin the current process to a specific NUMA node to improve memory locality
              during initialization or data ingestion:
                void pin_process_to_numa_node(int node_id);

              Query the number of logical CPUs associated with a NUMA node:
                int get_numa_node_cpu_count(int node_id);

        """)

    return textwrap.dedent(f"""\
        CPU affinity helpers is predefined in {filename}.
        You have to use the following functions, no need to implement them yourself,
        they are already provided by the runtime:

        {numa_section}CPU affinity:
          Pin the process to a single logical CPU for deterministic execution:
            void pin_process_to_cpu(int cpu_id);

          Restore affinity to all available CPUs:
            void unpin_process_from_cpus();
    """)
