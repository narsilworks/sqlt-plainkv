package sqltplainkv

import (
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {

	pkv := NewSQLtPlainKV("local.dat", false)

	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.Begin()

	if err := pkv.Set(`sample_key`, []byte(`Sample value`)); err != nil {
		t.Logf(`%s`, err)
		pkv.Rollback()
		t.Fail()
	}

	b, err := pkv.Get(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		pkv.Rollback()
		t.Fail()
	}

	t.Logf(`Retrieved from the database: %s`, b)

	err = pkv.Del(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		pkv.Rollback()
		t.Fail()
	}

	pkv.Commit()
	pkv.Close()
}

func TestOpenMime(t *testing.T) {

	pkv := NewSQLtPlainKV("local.dat?_pragma=journal_mode(WAL)", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.Begin()
	defer pkv.Rollback()

	if err := pkv.Set(`sample_key`, []byte(`Sample value`)); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.SetMime(`sample_key`, `application/json`)

	b, err := pkv.Get(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	mime, err := pkv.GetMime(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	t.Logf(`Retrieved from the database: %s as %s`, b, mime)

	pkv.Commit()

	pkv.Close()
}

func TestOpenListKeys(t *testing.T) {

	pkv := NewSQLtPlainKV("local.dat?_pragma=journal_mode(WAL)", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.Begin()
	defer pkv.Rollback()

	strs, err := pkv.ListKeys("sample")
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	for _, v := range strs {
		b, err := pkv.Get(v)
		if err != nil {
			t.Logf(`%s`, err)
			t.Fail()
		}

		t.Logf(`Retrieved from the database: %s`, b)
	}

	pkv.Commit()

	pkv.Close()
}

func TestIncrement(t *testing.T) {
	pkv := NewSQLtPlainKV("local.dat", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.Begin()
	defer pkv.Rollback()

	tally, err := pkv.Tally("sample", 0)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}
	t.Logf(`Initial tally: %d`, tally)

	for i := 0; i < 10; i++ {
		tally, err := pkv.TallyIncr("sample")
		if err != nil {
			t.Logf(`%s`, err)
			t.Fail()
		}

		t.Logf(`Last tally: %d`, tally)
	}

	pkv.Commit()

	pkv.Close()
}

func TestDecrement(t *testing.T) {
	pkv := NewSQLtPlainKV("local.dat", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.Begin()
	defer pkv.Rollback()

	tally, err := pkv.Tally("sample", 0)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}
	t.Logf(`Initial tally: %d`, tally)

	for i := 0; i < 10; i++ {
		tally, err := pkv.TallyDecr("sample")
		if err != nil {
			t.Logf(`%s`, err)
			t.Fail()
		}

		t.Logf(`Last tally: %d`, tally)
	}

	pkv.Rollback()
	pkv.Close()
}

func BenchmarkPerformance(b *testing.B) {

	pkv := NewSQLtPlainKV("local.dat?_pragma=journal_mode(WAL)", false)
	if err := pkv.Open(); err != nil {
		b.Logf(`%s`, err)
		b.Fail()
	}

	pkv.Begin()
	defer pkv.Rollback()

	for i := 0; i < 100000; i++ {
		if err := pkv.Set(`sample_key`+strconv.Itoa(i), []byte(`Sample value `+strconv.Itoa(i))); err != nil {
			b.Logf(`%s`, err)
			b.Fail()
		}
	}

	pkv.Commit()
	pkv.Close()
}
