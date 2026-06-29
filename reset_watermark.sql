SELECT MAX(last_modified) FROM public.cases;

UPDATE public.watermark 
SET last_modified = '2026-01-01 00:00:00' 
WHERE table_name = 'cases';

select * from public.watermark;


select * from cases;