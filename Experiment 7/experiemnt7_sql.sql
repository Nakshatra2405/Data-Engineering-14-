CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE roles (
    role_id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL UNIQUE,
    salary NUMERIC(10, 2) NOT NULL
);

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    hire_date DATE NOT NULL,
    role_id INT REFERENCES roles(role_id) ON DELETE RESTRICT,
    department_id INT REFERENCES departments(department_id) ON DELETE RESTRICT
);

INSERT INTO departments (department_id, name) VALUES 
(1, 'Sales'),
(2, 'Marketing'),
(3, 'Engineering'),
(4, 'Human Resources'),
(5, 'Finance')
ON CONFLICT (department_id) DO NOTHING;

INSERT INTO roles (role_id, title, salary) VALUES 
(1, 'Junior Associate', 45000.00),
(2, 'Analyst', 60000.00),
(3, 'Senior Analyst', 75000.00),
(4, 'Manager', 90000.00),
(5, 'Director', 120000.00)
ON CONFLICT (role_id) DO NOTHING;

SELECT * FROM employees;

SELECT
    e.first_name, e.last_name, r.title, d.name AS department_name, r.salary
FROM employees e
JOIN roles r ON e.role_id = r.role_id
JOIN departments d ON e.department_id = d.department_id
ORDER BY r.salary DESC
LIMIT 10;