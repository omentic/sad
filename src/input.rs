use {
  super::{
    argparse::{Arguments, Mode},
    types::Die,
    udiff::DiffRange,
  },
  futures::{
    future::{ready, Either},
    stream::{self, once, try_unfold, Stream, TryStreamExt},
    StreamExt,
  },
  glob::glob,
  regex::Regex,
  std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
  },
  tokio::{
    fs::{canonicalize, File},
    io::{AsyncBufReadExt, BufReader},
  },
};

#[derive(Debug)]
pub enum RowIn {
  Entire(PathBuf),
  Piecewise(PathBuf, HashSet<DiffRange>),
}

#[derive(Debug)]
struct DiffRow(PathBuf, DiffRange);

fn p_row(row: &str) -> Result<DiffRow, Die> {
  let f = || Die::ArgumentError(String::new());
  let ff = |_| f();
  let preg = "\x04 @@ -(\\d+),(\\d+) \\+(\\d+),(\\d+) @@$";
  let re = Regex::new(preg).map_err(Die::RegexError)?;
  let captures = re.captures(row).ok_or_else(f)?;

  let before_start = captures
    .get(1)
    .ok_or_else(f)?
    .as_str()
    .parse::<usize>()
    .map_err(ff)?;
  let before_inc = captures
    .get(2)
    .ok_or_else(f)?
    .as_str()
    .parse::<usize>()
    .map_err(ff)?;
  let after_start = captures
    .get(3)
    .ok_or_else(f)?
    .as_str()
    .parse::<usize>()
    .map_err(ff)?;
  let after_inc = captures
    .get(4)
    .ok_or_else(f)?
    .as_str()
    .parse::<usize>()
    .map_err(ff)?;

  let range = DiffRange {
    before: (before_start - 1, before_inc),
    after: (after_start - 1, after_inc),
  };
  let path = PathBuf::from(String::from(re.replace(row, "")));
  Ok(DiffRow(path, range))
}

async fn stream_patch(patches: &Path) -> impl Stream<Item = Result<RowIn, Die>> {
  let patches = patches.to_owned();

  let fd = match File::open(&patches).await {
    Err(e) => {
      let err = Die::IO(patches.clone(), e.kind());
      return Either::Left(once(ready(Err(err))));
    }
    Ok(fd) => fd,
  };
  let reader = BufReader::new(fd).split(b'\0');
  let acc = HashSet::new();

  let stream = try_unfold(
    (reader, patches, PathBuf::new(), acc),
    move |mut s| async move {
      let next = s
        .0
        .next_segment()
        .await
        .map_err(|e| Die::IO(s.1.clone(), e.kind()))?;

      match next {
        None if s.3.is_empty() => Ok(None),
        None => {
          let path = s.2;
          let ranges = s.3;
          s.2 = PathBuf::new();
          s.3 = HashSet::new();
          Ok(Some((Some(RowIn::Piecewise(path, ranges)), s)))
        }
        Some(buf) => {
          let row =
            String::from_utf8(buf).map_err(|_| Die::IO(s.1.clone(), ErrorKind::InvalidData))?;
          let parsed = p_row(&row)?;
          if parsed.0 == s.2 {
            s.3.insert(parsed.1);
            Ok(Some((None, s)))
          } else {
            let path = s.2;
            let ranges = s.3;
            s.2 = parsed.0;
            s.3 = HashSet::new();
            s.3.insert(parsed.1);
            if ranges.is_empty() {
              Ok(Some((None, s)))
            } else {
              Ok(Some((Some(RowIn::Piecewise(path, ranges)), s)))
            }
          }
        }
      }
    },
  );

  Either::Right(stream.try_filter_map(|x| ready(Ok(x))))
}

#[allow(clippy::useless_format)]
fn iter_paths(globs: impl IntoIterator<Item = String>) -> impl Iterator<Item = PathBuf> {
  globs
    .into_iter()
    .flat_map(|pattern| [format!("{pattern}"), format!("{pattern}/*")])
    .filter_map(|pattern| glob(&pattern).ok())
    .flatten()
    .filter_map(|path| path.ok().filter(|path| path.is_file()))
}

fn stream_files(files: impl Iterator<Item = PathBuf>) -> impl Stream<Item = Result<RowIn, Die>> {
  if false {
    return Either::Left(once(ready(Err(Die::Eof))));
  }
  let reader = stream::iter(files);
  let seen = HashSet::new();

  let stream = try_unfold((reader, seen), move |mut s| async move {
    match s.0.next().await {
      None => Ok(None),
      Some(path) => match canonicalize(&path).await {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Some((None, s))),
        Err(e) => Err(Die::IO(path, e.kind())),
        Ok(canonical) => Ok(Some({
          if s.1.insert(canonical.clone()) {
            (Some(RowIn::Entire(canonical)), s)
          } else {
            (None, s)
          }
        })),
      },
    }
  });

  Either::Right(stream.try_filter_map(|x| ready(Ok(x))))
}

#[allow(clippy::option_if_let_else)]
pub async fn stream_in(mode: &Mode, args: &Arguments) -> impl Stream<Item = Result<RowIn, Die>> {
  match mode {
    Mode::Initial => match &args.paths {
      None => { // todo: consider adding back support for stream editing?
        let current_dir = std::env::current_dir().expect("insufficient permissions").to_str().unwrap().to_owned();
        Either::Left(stream_files(iter_paths(vec![current_dir].into_iter())))
      },
      Some(paths) => Either::Left(stream_files(iter_paths(paths.clone().into_iter()))),
    },
    Mode::Preview(path) | Mode::Patch(path) => Either::Right(stream_patch(path).await),
  }
}
