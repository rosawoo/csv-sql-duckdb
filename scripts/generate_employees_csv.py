import argparse
import datetime
import os
import random


FIRST_NAMES = [
    "Alice",
    "Bob",
    "Carol",
    "David",
    "Eva",
    "Frank",
    "Grace",
    "Henry",
    "Ivy",
    "Jack",
    "Liam",
    "Mia",
    "Noah",
    "Olivia",
    "Priya",
    "Quinn",
    "Rosa",
    "Sam",
    "Tina",
    "Uma",
    "Victor",
    "Wen",
    "Xavier",
    "Yara",
    "Zoe",
]

LAST_NAMES = [
    "Johnson",
    "Martinez",
    "Lee",
    "Kim",
    "Smith",
    "Zhao",
    "Patel",
    "Nguyen",
    "Chen",
    "Brown",
    "Davis",
    "Wilson",
    "Garcia",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
]

DEPARTMENTS = [
    "Engineering",
    "Sales",
    "Marketing",
    "Finance",
    "HR",
    "Product",
    "Support",
    "Operations",
]


def _rand_date(rng: random.Random) -> str:
    start = datetime.date(2010, 1, 1).toordinal()
    end = datetime.date(2024, 12, 31).toordinal()
    return datetime.date.fromordinal(rng.randint(start, end)).isoformat()


def _make_row(i: int, rng: random.Random) -> str:
    name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    dept = rng.choice(DEPARTMENTS)
    salary = rng.randint(45_000, 220_000)
    hire_date = _rand_date(rng)
    return f"{i},{name},{dept},{salary},{hire_date}\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a synthetic employees CSV for load testing.")
    parser.add_argument("--out", default="employees_1gb.csv", help="Output path")
    parser.add_argument(
        "--bytes",
        type=int,
        default=1024 * 1024 * 1024,
        help="Target output size in bytes (default: 1 GiB)",
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    parser.add_argument(
        "--flush-bytes",
        type=int,
        default=4 * 1024 * 1024,
        help="Buffer size before flushing to disk (default: 4 MiB)",
    )

    args = parser.parse_args()

    rng = random.Random(args.seed)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        f.write("employee_id,name,department,salary,hire_date\n")

        i = 1
        buf: list[str] = []
        buf_bytes = 0

        while True:
            current_size = os.path.getsize(args.out)

            row = _make_row(i, rng)
            row_bytes = len(row.encode("utf-8"))

            # Avoid overshooting: if writing this row would exceed the target size,
            # flush what we have and stop.
            if current_size + buf_bytes + row_bytes > args.bytes:
                if buf:
                    f.write("".join(buf))
                    buf.clear()
                    buf_bytes = 0
                    f.flush()
                break

            buf.append(row)
            buf_bytes += row_bytes
            i += 1

            if buf_bytes >= args.flush_bytes:
                f.write("".join(buf))
                buf.clear()
                buf_bytes = 0
                f.flush()

    print(f"Wrote: {args.out} size: {os.path.getsize(args.out)} bytes")


if __name__ == "__main__":
    main()
