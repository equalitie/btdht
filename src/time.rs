use std::{
    fmt,
    ops::{Add, Sub},
    time::{Duration, Instant as StdInstant},
};

const WEEK_IN_SECONDS: u64 = 7 * 24 * 60 * 60;
const OFFSET: Duration = Duration::from_secs(WEEK_IN_SECONDS);

/// This `Instant` structure is basically the same thing as `std::time::Instant` but internally
/// shifted into the future by `OFFSET`. It is because on Windows we saw panics in this code:
///
/// Instant::now().checked_sub(15 minutes).unwrap()
///
/// Internally (at least on Windows) `Instant` is represented as `Duration` since some starting
/// point. This starting point is not known by us (it may be since the device booted up, since the
/// app started, since first use,...) and thus subtracting 15 minutes from the internal `Duration`
/// could result in a negative value, which is not allowed and thus the `checked_sub` would return
/// `None`.
///
/// We considered other options to circumvent this issue but they all have their own cons:
///
/// 1. `chrono::DateTime` and `std::time::SystemTime` are not monotonic.
/// 2. We could just use `Some(Instant)` and `None` in calculations, but this would require careful
///    refactor.
#[derive(Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub(crate) struct Instant {
    std_instant: StdInstant,
}

impl Instant {
    pub fn now() -> Self {
        Self {
            std_instant: StdInstant::now().checked_add(OFFSET).unwrap(),
        }
    }

    pub fn checked_sub(&self, rhs: Duration) -> Option<Self> {
        self.std_instant
            .checked_sub(rhs)
            .map(|std_instant| Self { std_instant })
    }
}

impl Add<Duration> for Instant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self {
        Self {
            std_instant: self.std_instant + rhs,
        }
    }
}

impl Sub<Duration> for Instant {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self {
        Self {
            std_instant: self.std_instant - rhs,
        }
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, rhs: Instant) -> Duration {
        self.std_instant - rhs.std_instant
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(instant) = self.std_instant.checked_sub(OFFSET) {
            instant.fmt(f)
        } else {
            f.write_fmt(format_args!("({:?} - one week)", self.std_instant))
        }
    }
}
