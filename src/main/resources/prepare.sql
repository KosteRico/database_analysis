create table logs (
timestamp_TW timestamp ,
uuid varchar(50),
ucid varchar(50),
upid varchar(50),
problem_number integer,
exercise_problem_repeat_session integer,
is_correct boolean,
total_sec_taken integer,
total_attempt_cnt integer,
used_hint_cnt integer,
is_hint_used boolean,
is_downgrade boolean,
is_upgrade boolean,
level integer
);