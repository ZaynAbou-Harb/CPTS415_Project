function generateNavbar(navbar_entries) {
    const currentPath = window.location.pathname.split('/').pop();
  
    return `
      <div class="topnav">
          ${navbar_entries.map(item => `
            <a href="${item.path}" class="${currentPath === item.path ? 'active' : ''}" ${item.style ? `style="${item.style}"` : ''} ${item.title ? `title="${item.title}"` : ''} ${item.id ? `id="${item.id}"` : ''}>
                ${item.icon ? `<i class="${item.icon}"></i>` : ''} ${item.img ? `<div class="topnav-pfp"><img src="${item.img}"></div>` : '' }  ${item.name}
            </a>
          `).join('')}
          <div class="search-container" style="float: left; position: relative;">
              <form id="search-form">
                  <input type="text" placeholder="Search.." name="search" id="search-input">
                  <button type="submit"><i class="fa fa-search"></i></button>
                  <div class="dropdown" style="position: absolute; background-color: white; border: 1px solid #ccc; z-index: 1000; width: 100%; display: none;"></div>
              </form>
          </div>
      </div>
    `;
  }
  
  document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.querySelector('#search-input');
    const searchForm = document.querySelector('#search-form');
    const searchContainer = document.querySelector('.topnav .search-container');
  
    // Create dropdown at the global level
    const dropdown = document.createElement('div');
    dropdown.className = 'dropdown-global';
    dropdown.style.display = 'none';
    dropdown.style.position = 'absolute';
    dropdown.style.zIndex = '1000';
    dropdown.style.backgroundColor = 'white';
    dropdown.style.border = '1px solid #ccc';
    dropdown.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.1)';
    dropdown.style.maxHeight = '200px';
    dropdown.style.overflowY = 'auto';
    dropdown.style.width = `${searchInput.offsetWidth}px`;
    document.body.appendChild(dropdown);
  
    searchInput.addEventListener('focus', () => {
      const rect = searchInput.getBoundingClientRect();
      dropdown.style.top = `${rect.bottom + window.scrollY}px`;
      dropdown.style.left = `${rect.left + window.scrollX}px`;
      dropdown.style.display = 'block';
    });
  
    searchInput.addEventListener('input', async () => {
      const query = searchInput.value.trim();
      if (query.length === 0) {
        dropdown.style.display = 'none';
        dropdown.innerHTML = '';
        return;
      }
  
      try {
        // Fetch search results from the backend
        const response = await fetch(`/search?q=${query}`);
        const results = await response.json();
  
        dropdown.innerHTML = '';
  
        if (results.length === 0) {
          const noResultsItem = document.createElement('div');
          noResultsItem.textContent = 'No matching pages found.';
          noResultsItem.style.padding = '8px';
          noResultsItem.style.color = 'red';
          dropdown.appendChild(noResultsItem);
          dropdown.style.display = 'block';
          return;
        }
  
        results.forEach(result => {
          const item = document.createElement('div');
          item.textContent = result.name;
          item.style.padding = '8px';
          item.style.cursor = 'pointer';
          item.addEventListener('click', () => {
            window.location.href = result.url; // Navigate to the selected page
          });
          dropdown.appendChild(item);
        });
  
        dropdown.style.display = 'block';
      } catch (error) {
        console.error('Error fetching search results:', error);
      }
    });
  
    searchForm.addEventListener('submit', async (event) => {
      event.preventDefault(); // Prevent default form submission
  
      const query = searchInput.value.trim().toLowerCase();
  
      // Check if the input is empty
      if (!query) {
          alert('Please enter the content you want to search.');
          window.location.href = '/'; // Redirect to the home page
          return;
      }
  
      // Fetch search results
      const response = await fetch(`/search?q=${query}`);
      const results = await response.json();
  
      // Handle no matching results
      if (results.length === 0) {
          alert('The page you are looking for does not exist. Redirecting to the home page.');
          window.location.href = '/'; // Redirect to the home page
          return;
      }
  
      // Check for an exact match
      const exactMatch = results.find(result => result.name.toLowerCase() === query);
  
      if (exactMatch) {
          window.location.href = exactMatch.url; // Redirect to the exact match
          return;
      }
  
      // Show dropdown for multiple results
      dropdown.innerHTML = results.map(result => `
          <div style="padding: 8px; cursor: pointer;" onclick="window.location.href='${result.url}'">
              ${result.name}
          </div>
      `).join('');
      dropdown.style.display = 'block';
  });
    
    
});
  
