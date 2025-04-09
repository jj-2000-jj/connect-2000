/**
 * Formats a date string in a human-readable format
 */
function formatDateString(date) {
  return new Date(date).toLocaleDateString('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });
}

/**
 * Groups emails by date
 */
function groupEmailsByDate(emails) {
  const grouped = {};
  
  emails.forEach(email => {
    const dateStr = email.date.toISOString().split('T')[0];
    if (!grouped[dateStr]) {
      grouped[dateStr] = [];
    }
    grouped[dateStr].push(email);
  });
  
  return grouped;
}

module.exports = {
  formatDateString,
  groupEmailsByDate
};